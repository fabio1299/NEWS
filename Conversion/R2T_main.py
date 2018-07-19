#! /usr/bin/env python3
#

"""
R2T_main.py
"""

from R2T_readinputfiles import LoadDischarge,AssembleData,\
    read_cap_deltas,get_gen_input,SaveData, \
    read_retirements, SaveHydro,SaveCapDelta,SaveGeneration, \
    getCoordID,loadCoordID,getCoordStr,loadCombID
from R2T_capacity import capacity, saveCap, saveEff # read_plants, read_retirements, read_cells,
from R2T_generation import  generation, saveGen
from R2T_postprocessing import genGDBC
from R2T_getreedsinput import createScript
import argparse
from argparse import RawTextHelpFormatter
import os
import sys
import glob
import R2T_Globals
from R2T_Globals import *



def SplitDoubleReEDSFileInput(input):
    InputItems=input.split(' ')
    if len(InputItems) != 4:
        print('Error parsing ReEDS input files: expected 4 parameters, got {}'.format(len(InputItems)))
        sys.exit(1)
    cap=''
    gen=''
    i=0
    while cap=='' or gen=='':
        if InputItems[i] == '-c':
            cap=InputItems[i+1]
        elif InputItems[i] == '-g':
            gen = InputItems[i + 1]
        elif i > 3:
            print('Error parsing ReEDS input files: check syntax (valid options are -c and -g)')
            sys.exit()
        i+=1
    return cap,gen

def ReEDS2TP2M(args,ReEDSlist=None,GDBCFiles=True):
    # For debug purposes, set the Verbose flag (e.g True)
#    global print_if_flag

    try:
        R2T_Globals.print_if_flag
    except NameError:
#    if 'print_if_flag' not in globals():
        init()

    R2T_Globals.print_if_flag = args.print_if_flag

    if ReEDSlist is not None and ReEDSlist.empty:
        # There are no files to be generated
        print_if('No new files need to be generated to run TP2M from existing ReEDS results')
        return

    R2T_Globals.YEAR_S=args.year
    R2T_Globals.YRS = [x for x in range(R2T_Globals.YEAR_S, R2T_Globals.YEAR_E + 1, 2)]

    if args.saveIntermediate:
        R2T_Globals.SavePP=False
        R2T_Globals.SaveCells=False
        R2T_Globals.SaveDeltas=False
        R2T_Globals.SaveCap=False
        R2T_Globals.SaveGen=False
        for c in args.saveIntermediate:
            if c=='P':
                R2T_Globals.SavePP = True
            elif c=='N':
                R2T_Globals.SaveCells = True
            elif c=='D':
                R2T_Globals.SaveDeltas = True
            elif c=='C':
                R2T_Globals.SaveCap = True
            elif c=='G':
                R2T_Globals.SaveGen = True
            else:
                print('Warning: Unrecognized intermediate file code {}. Defaulting to not saving any intermediate files'.format(c))
                R2T_Globals.SavePP=False
                R2T_Globals.SaveCells=False
                R2T_Globals.SaveDeltas=False
                R2T_Globals.SaveCap=False
                R2T_Globals.SaveGen=False

    """""""""""""""
    input -> dataframes for capacity 
    """""""""""""""
    # We first read the Power Plant database table and calculate the aggregate installed capacity by PCA, fuel and cooling
    plantsByID = AssembleData(args.powerplants, args.capacity, args.efficiency, args.year, args.lastyear)
    # We load the retirement table
    retirementYears = read_retirements(args.retire_file)
    # and finally we laod the netwrok cells and update the power plant table with additional info conimg from the
    # network cells (e.g. occupancy, available discharge)
    plantsByID,cells, sums = LoadDischarge(plantsByID,args.netcells)
    #	print(df_data['powerplants'].loc[:,['EIA_ID','NP_2010','PrimeMover','Fuel','Cooling','Eff_2010','PCA','withdrawal','avg_sum']])

    # Now we check if we are processing a batch of files from the link server (e.g. ReEDSlist is a DataFrame)
    # or if we are processing on individual file or a folder with some ReEDS result files
    # In the later case we load the file names in the ReEDSlist DataFrame:
    # if the ReEDS file input parameter is a directory we have a multifile
    # analysis

    if ReEDSlist is None:
        InputList=True
        if len(args.ReedsResults.split()) == 1:
            AbsPath = os.path.abspath(args.ReedsResults)
            if os.path.isdir(args.ReedsResults):
                reedsin=glob.glob(AbsPath+'/*.xls?')
            else:
                reedsin =[AbsPath]
        else:
            reedsCapFile, reedsGenFile = SplitDoubleReEDSFileInput(args.ReedsResults)
            reedsCapFile = os.path.abspath(reedsCapFile)
            reedsGenFile = os.path.abspath(reedsGenFile)
            reedsin=['CSV']
        ReEDSlist = pd.DataFrame(reedsin,columns=['InputList'])
    else:
        InputList=False

        #tmp=pd.DataFrame()
        #tmp['tmp']=ReEDSlist['path'] + '/' + ReEDSlist['version'] + '/ReEDS2TP2M/ReEDSresults/Water_output.xlsx'
        #reedsin=list(tmp['tmp'])

    # we can now run the capacity allocation routine on all the files (one at the time)
    #for infile in reedsin:
    for InReEDS_index,InReEDS in ReEDSlist.iterrows():
        if InputList:
            infile = InReEDS['InputList']
            if infile != 'CSV':
                infileBase = os.path.basename(infile)
                fileName, fileExtension = os.path.splitext(infileBase)
                if infileBase == 'Water_output.xlsx':
                    infileBase = 'in: ' + os.path.dirname(infile)
            else:
                infileBase = reedsCapFile.split('/')[-2]
                fileExtension = '.csv'
        else:
            if InReEDS['unmodeled'] == False:
                print_if('Skipping ReEDS import:')
                print_if('\tReEDS data already imported for GCM: {}, RCP:{}, Scenario: {}, Version: {}'.format(InReEDS['gcm'],InReEDS['rcp'],InReEDS['scenario'],InReEDS['version']))
                continue
            CurrDir=InReEDS['path'] + '/' + InReEDS['version'] + '/ReEDS2TP2M/ReEDSresults/'
            R2T_Globals.OutputDir = CurrDir.replace('ReEDSresults/', 'TP2Minput/')
            make_sure_path_exists(R2T_Globals.OutputDir)
            infileBase=InReEDS['DirectoryName']
            if InReEDS['Excel']=='Excel':
                infile = CurrDir+ 'Water_output.xlsx'
                fileExtension == '.xlsx'
            else:
                if InReEDS['Excel'] == 'GDX':
                    infile = CurrDir + 'water_output.gdx'
                    reedsCapFile = infile
                    reedsGenFile = infile
                    fileExtension = '.gdx'
                else:
                    reedsCapFile = CurrDir+ 'convqctn.csv'
                    reedsGenFile = CurrDir+ 'convqctmnallm.csv'
                    fileExtension = '.csv'
                    infile = infileBase

        print_if('Processing ReEDS simulation data {}'.format(infileBase))
        if fileExtension=='.xls' or fileExtension=='.xlsx':
            print_if('Loading file {}'.format(infile)) # Base))
            reedsCapFile=read_excel(infile)
            reedsGenFile=reedsCapFile #read_reeds(args.ReedsResults)
            #R2T_Globals.OutputDir="../TP2MInput/"+fileName+"/"
            #R2T_Globals.OutputDir = '/asrc/ecr/NEWS/LoopingPaper/tmp/TP2Minput/'
############## NOTE: DELETE OR COMMENT THE LINE ABOVE AFTER DEBUGGING ##########################
#        else:

        R2T_Globals.SaveDir=R2T_Globals.OutputDir.replace('TP2Minput/', 'R2Tsave/')
#        R2T_Globals.SaveDir=R2T_Globals.OutputDir + 'R2Tsave/'

        make_sure_path_exists(R2T_Globals.SaveDir)

        # We save the intermediate Power Plants and the network tables, if rquired
        if R2T_Globals.SavePP:
            SaveData(R2T_Globals.SaveDir + args.OutFile, plantsByID)
        if R2T_Globals.SaveCells:
            SaveHydro(R2T_Globals.SaveDir + args.outnetcells, cells)

        # Now we can read the ReEDS resuts, integrate it with the aggregates capacities and then calculate the yearly deltas
        print_if('Computing capacity Deltas table')
        capDeltas = read_cap_deltas(reedsCapFile, sums.copy(),file_type=fileExtension)  # key -> yr -> MW
        # We save the intemediate capacity deltas file, if required
        if R2T_Globals.SaveDeltas:
            SaveCapDelta(R2T_Globals.SaveDir + "ReEDS_capdelta.xlsx", capDeltas.reset_index(level=['PCA', 'Fuel', 'Cooling']))

        print_if('Computing capacity allocation')
        capoutput, effoutput = capacity(plantsByID.copy(), capDeltas, retirementYears.copy(), cells.copy(), sums.copy())
        # and save the result in both a CSV and and EXcel file
        saveCap(capoutput, R2T_Globals.SaveDir, )  # add str to change filename
        saveEff(effoutput, R2T_Globals.SaveDir, )  # add str to change filename

        # Now we can load the ReEDS generation data
        print_if('Computing generation allocation')
        geninput  = get_gen_input(reedsGenFile,file_type=fileExtension)
        if R2T_Globals.SaveGen:
            SaveGeneration(R2T_Globals.SaveDir + "ReEDS_gen.xlsx", geninput)
        # And we run the generation allocation routine
        genoutput = generation(geninput, capoutput, )  # add str to change filename
        saveGen(genoutput, R2T_Globals.SaveDir,)  # add str to change filename

        if GDBCFiles:
            outpath=R2T_Globals.OutputDir  # + '/PPgdbc'
            print_if('Creating GDBC files in path {}'.format(outpath))

            make_sure_path_exists(outpath)

            print_if('Adjusting capacity, efficiency and generation tables')

            # We add the CoordID to all the files
            capoutput['CoordID'] = capoutput.apply(loadCoordID, axis=1)
            effoutput['CoordID'] = effoutput.apply(loadCoordID, axis=1)
            genoutput['CoordID'] = genoutput.apply(loadCoordID, axis=1)

            # And the CombID
            capoutput['CombID'] = capoutput.apply(loadCombID, axis=1)
            effoutput['CombID'] = effoutput.apply(loadCombID, axis=1)
            genoutput['CombID'] = genoutput.apply(loadCombID, axis=1)

            genGDBC(capoutput, effoutput, genoutput, R2T_Globals.GDBCtemplate, 2010, 2050, outpath,True)
        else:
            print_if('Skipping GDBC files creation')

        EndFile=infile

        if infile=='CSV':
            EndFile = infileBase


        if not InputList:
            if InReEDS['unscripted'] == True:
                print_if('Generating WBM TP2M run script for simulation {}'.format(EndFile))
                createScript(InReEDS)
        else:
            print_if('WBM TP2M run script not created for ReEDS simulation {}'.format(EndFile))

        print_if('Finished processing ReEDS simulation {}'.format(EndFile)) #Base))

    return


if __name__ == "__main__":

    """""
    main
    """""
    parser = argparse.ArgumentParser(description='Allocates ReEDS output to a WBM network',
                                             formatter_class=RawTextHelpFormatter)

    parser.add_argument('-c', '--capacity',
                                dest='capacity',
                                help="Input file: Power Plants capacity",
                                default="/asrc/ecr/NEWS/PowerPlants/Capacity.csv")  # _AllYrs.csv") #
            # default = "/asrc/ecr/NEWS/PowerPlants/Capacity_AllYrs.csv")
            #    	default='./test2000.gdbc')

    parser.add_argument('-o', '--output',
                                dest='OutFile',
                                help="Output file: pre-processed Power Plant table (Only used with option -S set to P)",
                                default='powerplants.csv')

    parser.add_argument('-r', '--reedsresults',
                                dest='ReedsResults',
                                help="Input file: ReEDS result file or folder\n" +
                                     "If Capacity and Generation are given as separate files\n" +
                                     "use the following format:\n" +
                                     "\t\"-c <capacity file name> -g <generation file name>\"\n" +
                                     "NOTE THE QUOTES!!!",
                                #default='/asrc/ecr/NEWS/LoopingPaper/gfdl-esm2m/rcp2p6/cap/v000/ReEDS2TP2M/ReEDSresults/Water_output.xlsx')
                                # default='./ReEDSResults')
                                # default='./ReEDSResults/Water_output_AAC.HadGemES85v1m10.CIRAoffNoWat.xlsx')
                                #default='./ReEDSResults/Water_output_HadGEM2_ES_RCP8p5_CAP.xlsx')
                                default='-c ./ReEDSResults/GFDL_ESM2M_RCP2p6_CAP/convqctn.csv -g ./ReEDSResults/GFDL_ESM2M_RCP2p6_CAP/convqctmnallm.csv')
    parser.add_argument('-d', '--retirements',
                                dest='retire_file',
                                help="Input file: Power Plants retirements table",
                                default='/asrc/ecr/NEWS/PowerPlants/Retirements.csv')

    parser.add_argument('-e', '--efficiency',
                                dest='efficiency',
                                help="Input file: Power Plants efficiency",
                                default='/asrc/ecr/NEWS/PowerPlants/Efficiency.csv')  # _AllYrs.csv') #
            # default = '/asrc/ecr/NEWS/PowerPlants/Efficiency_AllYrs.csv')

    parser.add_argument('-n', '--innetcells',
                                dest='netcells',
                                help="Input file: network cells table with discharge",
                                default='/asrc/ecr/NEWS/MultiScenario/Network/HydroCells.csv')
            # default = './PowerPlants/HydroCells.xlsx')

    parser.add_argument('-f', '--outnetcells',
                                dest='outnetcells',
                                help="Output file: per-processed network cells table (Only used with option -S set to N)",
                                default='HydroCells.csv')

    parser.add_argument('-p', '--powerplants',
                                dest='powerplants',
                                help="Input file: Power Plants database table",
                                default='/asrc/ecr/NEWS/PowerPlants/PowerPlantChars.csv')  # _AllYrs.csv') #
            # default = '/asrc/ecr/NEWS/PowerPlants/PowerPlantChars_AllYrs.csv')

    parser.add_argument('-y', '--year',
                                dest='year',
                                help="Capacity and efficency start year used to build file",
                                default=2010)

    parser.add_argument('-l', '--last',
                                dest='lastyear',
                                help="Capacity and efficency end year in input file",
                                default=2015)

    parser.add_argument('-V', '--verbose',
                                dest='print_if_flag',
                                action='store_true',
                                help="Prints some debugging info",
                                default=False)

    parser.add_argument('-S', '--saveIntermediate',
                                dest='saveIntermediate',
                                help="Save intermediate files: \n" +
                                     "\tP - Power Plants\n" +
                                     "\tN - Newtwork Cells\n" +
                                     "\tD - Cap Deltas\n" +
                                     "\tC - ReEDS capacity\n" +
                                     "\tG - ReEDS generation",
                                default=False)

    args = parser.parse_args()

    global print_if_flag
    print_if(args)

#    R2T_Globals.OutputDir = '/asrc/ecr/NEWS/LoopingPaper/hadgem2-es/rcp8p5/nc_bau/v0/ReEDS2TP2M/TP2Minput/'

    R2T_Globals.OutputDir = '/asrc/ecr/NEWS/configurations/Link/tmp/TP2Minput/'

#    '/asrc/ecr/NEWS/configurations/Link/ReEDSResults/'

    ReEDS2TP2M(args,GDBCFiles=False)
