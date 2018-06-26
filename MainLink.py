#! /usr/bin/env python3
#

"""
Service MainLink.py
Main program that dispatches the TP2M-ReEDS tasks to the appropriate
routines
"""

from __future__ import print_function

import gdxpds
import rgis as rg
import time
import argparse
from argparse import RawTextHelpFormatter
import os.path
import numpy as np
import Pr_CDD_HDD as chdd
import Pr_PCA_WaterStats as WatStat
from R2T_main import ReEDS2TP2M
from R2T_getreedsinput import runs, createScripts
import R2T_Globals
from R2T_Globals import *
from T2R_poweroutput import T2R_CalcAAC

R2T_Globals.init()

global print_if_flag
import subprocess as sp

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

# CONSTANTS
PCA_Grid = '/asrc/ecr/NEWS/MultiScenario/Network/PCA_REG.gdbc'


# The following is a small routine that can help with debugging
# def print_if(value):
#    if print_if_flag:
#        print(value)
#        sys.stdout.flush()

# For debug purposes, set to True
# print_if_flag=True


# The following are the main functions called by the --task option of the
# command line

def prime(**kwargs):
    print_if('ReEDS priming started: {}'.format(R2T_Globals.CurTime()))

    # Save the start time to monitor how much time it takes...
    start_time = time.time()

    # Set the years of interest for output
    year_of_interest_start = 2010  # 1972 #
    year_of_interest_end = 2050  # 1980 #
    year_of_interest_step = 2

    # Get the start and end years of the WBM analysis
    start = int(wbm_run.EnvDict['STARTYEAR'])
    end = int(wbm_run.EnvDict['ENDYEAR'])

    # Initialize the smoothing window, the parameter is read from the input line
    # and it's either 10, 20 or 30 (Defualt 20)
    # The shift number is equal to the half the size of the window minus 1 multiplied
    # by -1
    # w_and_p_num=4 ;shift_number=-1 #(change for different averages, 10yr=10,-4 | 20yr=20,-9 | 30yr=30,-14
    # this is equivalent to the center=True option with the new method .rolling of the dataframe

    w_and_p_num = int(args.window)
    b_number = int((w_and_p_num / 2))
    a_number = int((w_and_p_num / 2 - 1))
    # shift_number = int((args.window / 2 - 1) * -1)

    print_if('Checking that a moving average window size of {} years is consistent with the analysis dataset'.format(
        w_and_p_num))

    # Set the start and end year based on the years of interest and the averaging window
    # in order
    start_needed = year_of_interest_start - b_number
    end_needed = year_of_interest_end + a_number

    # Check that both the first and the last year of interest are within the
    # years of the analysis

    if not ((start <= start_needed <= end) & (start <= end_needed <= end)):
        raise ('Range of years needed {}-{} not fully contained in analysis years {}-{}. Check moving window size'.
               format(start_needed, end_needed, start, end))

    # Get the out folder name for the ReEDS priming datasets
    # from the WBM configuration setttings
    outputFolder = wbm_run.EnvDict['NEWSLINKDIR'] + '/' + \
                   wbm_run.EnvDict['GCM'] + '/' + \
                   wbm_run.EnvDict['RCP'] + '/' + \
                   wbm_run.EnvDict['EXPERIMENT'] + '/'
    if wbm_run.EnvDict['RUNVER'] != '':
        outputFolder = outputFolder + wbm_run.EnvDict['RUNVER'] + '/'
    outputFolder = outputFolder + 'WBM2ReEDS'

    # Check if folder already exists, otherwise create it

    print_if('Making sure that the output folder {} exists'.format(outputFolder))

    if sys.version_info[0] < 3:
        R2T_Globals.mkdir_p(outputFolder)
    else:
        os.makedirs(outputFolder, exist_ok=True)
    # and set the access rights to the folder, but only if the
    # script is being run with the same uid as that of the
    # folder owner (otherwise we get an error)
    if os.stat(outputFolder).st_uid == os.getuid():
        os.chmod(outputFolder, 0o775)

    print_if('Output folder set to {}'.format(outputFolder))

    # We first load the PCA raster.
    # The PCA mask is a simple is currently read from the file
    # /asrc/ecr/NEWS/MultiScenario/Network/PCA_REG.gdbc to reduce
    # dataset redundancy
    #    print_if('Loading PCA regions')
    #    pca_file="../../Tests/PcaRegions.bil" # #
    #    with open(pca_file, "rb") as ifile:
    #        pca = np.fromfile(ifile, dtype=np.int16)
    # Unfortunately numpy does not have a nan value for integers, so we can
    # only set the NoData value (32767) to False (e.g. 0) and then we can
    # filter it out.
    # We could as well simply skip setting it to False and just ignore it
    # in the .unique statement (e.g. pcas=np.unique(pca[pca != 32767])
    # Setting it to 0 is more consistent when viewing the data...

    #    pca[pca==32767]=False

    # Load data from gdbc file
    print_if('Loading PCA raster')
    rgPCA = rg.grid(PCA_Grid, False)  # clip2.gdbc' # test2000b.gdbc'
    rgPCA.Load()
    pca = np.nan_to_num(rgPCA.Data[0]).astype(int)

    # Getting the unique values of the zones
    pcas = np.unique(pca[pca != 0])

    DictCddHdd = {wbm_run.EnvDict['RUN']: {}}
    GridPath = wbm_run.OutDict['AirTemperature'][0].replace('TimeStep', 'Daily')
    GridFile = wbm_run.OutDict['AirTemperature'][1].replace('TimeStep', 'd')
    DischargeBase = wbm_run.OutDict['Discharge'][0].replace('TimeStep', 'Monthly') + wbm_run.OutDict['Discharge'][
        1].replace('TimeStep', 'm')
    DischargeBase = DischargeBase.replace('0000.gdbc', '')
    RunoffBase = wbm_run.OutDict['Runoff'][0].replace('TimeStep', 'Monthly') + wbm_run.OutDict['Runoff'][1].replace(
        'TimeStep', 'm')
    RunoffBase = RunoffBase.replace('0000.gdbc', '')
    modelname = wbm_run.EnvDict['GCM']
    scenario = wbm_run.EnvDict['RCP'] + '_' + wbm_run.EnvDict['EXPERIMENT']
    if wbm_run.EnvDict['RUNVER'] != '':
        scenario = scenario + '_' + wbm_run.EnvDict['RUNVER']

    outFileBase = outputFolder + '/'  # + wbm_run.EnvDict['RUN']

    # We first generate the HDD and CDD file for the given run

    print_if('  HDD & CDD started: {}'.format(R2T_Globals.CurTime()))

    outFileHCDD = outputFolder + '/' + 'HDDCDD.csv'
    chdd.cdd_hdd(GridPath, GridFile,
                 start_needed, end_needed,
                 pca, w_and_p_num,
                 year_of_interest_start, year_of_interest_end, year_of_interest_step,
                 modelname + '_' + scenario, outFileHCDD, print_if_flag)

    print_if('  HDD & CDD finished: {}'.format(R2T_Globals.CurTime()))

    print_if('  Water availability started: {}'.format(R2T_Globals.CurTime()))
    # Years range for historical water availablity calculations
    Hist_year_start = 1985
    Hist_year_end = 2005

    WSdata = WatStat.PCAStatInput(DischargeBase, RunoffBase, min(Hist_year_start, start_needed),
                                  max(Hist_year_end, end_needed), outFileBase, print_if_flag)

    WatStat.PCAStats(modelname, scenario, WSdata,
                     w_and_p_num, b_number, a_number,
                     year_of_interest_start, year_of_interest_end,
                     Hist_year_start, Hist_year_end,
                     outFileBase, print_if_flag)

    ts = time.time()
    print_if('  Water availability finished: {}'.format(R2T_Globals.CurTime()))
    print_if("ReEDS priming input completed in --- %s minutes ---" % ((ts - start_time) / 60))
    print_if('ReEDS priming finished: {}'.format(R2T_Globals.CurTime()))


def reeds(**kwargs):
    print_if('AAC files generation started: {}'.format(R2T_Globals.CurTime()))

    # Save the start time to monitor how much time it takes...
    start_time = time.time()

    start_yr = R2T_Globals.YEAR_RE_S  # 2010
    end_yr = R2T_Globals.YEAR_RE_E  # 2050
    #    ST_YR = 2010
    #    END_YR = 2050
    # SEASONS = R2T_Globals.Seasons #  ["Winter", "Spring", "Summer", "Fall"]
    variable = "poweroutputtotal"

    MODEL = wbm_run.EnvDict['GCM']  # "hadgem2-es"
    RCP = wbm_run.EnvDict['RCP']  # "rcp2p6"
    REEDS = wbm_run.EnvDict['EXPERIMENT']  # "nc_bau"
    VERSION = wbm_run.EnvDict['RUNVER']  # "v000"

    base_dir = wbm_run.EnvDict['PROJECTDIR'] + (
                "/%s/%s/%s/%s" % (MODEL, RCP, REEDS, VERSION))  # /asrc/ecr/NEWS/LoopingPaper

    pp_f = base_dir + "/ReEDS2TP2M/R2Tsave/capoutput.csv"

    out_dir = args.outputFolder  # '/asrc/ecr/fabio/NEWS/test'

    if out_dir != '':
        out_dir = out_dir + ("/%s/%s/%s/%s" % (MODEL, RCP, REEDS, VERSION))

    powerplants = pd.read_csv(pp_f, header=0)
    powerplants['Comb_ID'] = powerplants.PlantCode.astype(int).astype(str) + \
                             '_' + powerplants.Fuel.astype(int).astype(str) + \
                             '_' + powerplants.Cooling.astype(int).astype(str)
    powerplants = powerplants.set_index(powerplants.Comb_ID)

    scenario = ("%s^%s^%s^%s" % (MODEL, RCP, REEDS, VERSION))

    T2R_CalcAAC(scenario, powerplants, variable, start_yr, end_yr, base_dir, out_dir)

    ts = time.time()
    print_if('AAC files generation completed in --- %s minutes ---' % ((ts - start_time) / 60))
    print_if('AAC files generation finished: {}'.format(R2T_Globals.CurTime()))


def tp2m(**kwargs):
    ReEDSlist = runs()

    print_if('Generating files to run TP2M from ReEDS results')

    tmp_args = argparse.Namespace()

    tmp_args.capacity = wbm_run.EnvDict['InstalledCapacity']  # '/asrc/ecr/NEWS/PowerPlants/Capacity.csv'

    tmp_args.retire_file = wbm_run.EnvDict['RetirementsFile']  # '/asrc/ecr/NEWS/PowerPlants/Retirements.csv'

    tmp_args.efficiency = wbm_run.EnvDict['Efficiency']  # '/asrc/ecr/NEWS/PowerPlants/Efficiency.csv'

    tmp_args.netcells = wbm_run.EnvDict['HydronetCells']  # '/asrc/ecr/NEWS/MultiScenario/Network/HydroCells.csv'

    tmp_args.OutFile = wbm_run.EnvDict['PowerPlantsOutSave']  # 'powerplants.csv'

    tmp_args.powerplants = wbm_run.EnvDict['PowerPlantsDB']  # '/asrc/ecr/NEWS/PowerPlants/PowerPlantChars.csv'

    tmp_args.outnetcells = wbm_run.EnvDict['HydronetCellsSave']  # 'HydroCells.csv'
    #                        Output file: per-processed network cells table (Only used with option -S set to N)

    tmp_args.year = int(wbm_run.EnvDict['PP_YearStart'])  # 2010

    tmp_args.lastyear = int(wbm_run.EnvDict['PP_YearEnd'])  # 2015

    # print("The value set for the variable print_if_flag is: {}".format(R2T_Globals.print_if_flag))
    tmp_args.print_if_flag = R2T_Globals.print_if_flag

    tmp_args.saveIntermediate = wbm_run.EnvDict['SaveIntermediate']  # 'CPD'
    #                            Save intermediate files:
    #                                 P - Power Plants
    #                                 N - Newtwork Cells
    #                                 D - Cap Deltas
    #                                 C - ReEDS capacity
    #                                 G - ReEDS generation

    ReEDS2TP2M(tmp_args, ReEDSlist.loc[ReEDSlist['unimported']])

    #  Now we create the script for any combination of GCM, RCP, Scenario and Version that
    # doesn't already have one
    if len(ReEDSlist.loc[ReEDSlist['unscripted']]) > 0:
        #    for index,row in ReEDSlist.iterrows():
        #        scriptFile=R2T_Globals.ProjectBase + 'Scripts/'+ row['script'] + '_' + row['version'] +'.sh'
        #        if not os.path.isfile(scriptFile):
        createScripts(ReEDSlist.loc[ReEDSlist['unscripted']])

    return  # remove once debugged

    # for index,row in ReEDSlist.loc[ReEDSlist['unimported']].iterrows():
    #    R2T_Globals.OutputDir = row['path'] + '/' + row['version'] + '/ReEDS2TP2M/TP2Minput/'
    #    tmp_args.ReedsResults = row['path'] + '/' + row['version'] + '/ReEDS2TP2M/ReEDSresults/Water_output.xlsx'
    # './ReEDSResults'
    # './ReEDSResults/Water_output_AAC.HadGemES85v1m10.CIRAoffNoWat.xlsx'
    #                Input file: ReEDS result file or folder
    #                            If Capacity and Generation are given as separate files
    #                            use the following format:
    #                            "-c <capacity file name> -g <generation file name>"
    #                            NOTE THE QUOTES!!!


def env(**kwargs):
    print('Environment loaded from script {}'.format(args.script))
    print('\n\nWBM run settings:\n')
    if sys.version_info[0] < 3:
        for key, value in wbm_run.EnvDict.iteritems():
            print(key + " = " + value)
    else:
        for key, value in wbm_run.EnvDict.items():
            print(key + " = " + value)
    print('\nOuputs requested:\n')
    if sys.version_info[0] < 3:
        for key, value in wbm_run.OutDict.iteritems():
            print(key)
            print_if(value)
    else:
        for key, value in wbm_run.OutDict.items():
            print(key)
            print_if(value)


# We first read the command line arguments and check that we have all the info needed
# to run the routine

parser = argparse.ArgumentParser(description='Main interface for the link between WBM/TP2M and ReEDS',
                                 formatter_class=RawTextHelpFormatter)

parser.add_argument('script',
                    help="WBM script (WBM*.sh) or Looping paramenters file (<project name>.prm)")

parser.add_argument('-t', '--task',
                    choices=['wbm', 'reeds', 'tp2m', 'prime', 'env'],
                    help="wbm = Runs WBM with newly created environment script (with -T option) \n" +
                         "prime = Primes ReEDS with WBM data \n" +
                         "reeds = Prepares ReEDS input from TP2M run \n" +
                         "tp2m  = Prepares TP2M input from ReEDS output and run TP2M \n" +
                         "loop  = loops between TP2M and ReEDS models \n" +
                         "env = Prints WBM script configuration environment \n" +
                         "Default is: env",
                    default='env')

parser.add_argument('-T', '--template',
                    dest='template',
                    help="WBM template script to generate new runs \n" +
                         "\tDefault is: /asrc/ecr/NEWS/configurations/WBMtemplate.sh",
                    default='/asrc/ecr/NEWS/configurations/WBMtemplate.sh')

parser.add_argument('-o', '--output',
                    dest='outputFolder',
                    help="Folder for the output of the rutine \n" +
                         "\tDefault is: /asrc/ecr/fabio/NEWS/test" +
                         "\tAutomatically sets the flag for split output (see wbm class documentation)",
                    default='')

parser.add_argument('-w', '--window',
                    choices=[10, 20, 30],
                    help="Number of years for moving average (smoothing) during priming\n" +
                         "only used when --task is set to 'prime' \n" +
                         "Deafault value is: 20",
                    default=20)

parser.add_argument('-V', '--verbose',
                    dest='print_if_flag',
                    action='store_true',
                    help="Prints some debugging info",
                    default=False)

parser.add_argument('-l', '--logfile',
                    dest='logfile',
                    help="Writes output to logfile, implies -V (verbose) flag",
                    default="")

args = parser.parse_args()

# global print_if_flag

if args.logfile != '':
    args.print_if_flag = True

# Initialize global variables

try:
    R2T_Globals.print_if_flag
except NameError:
    #    if 'print_if_flag' not in globals():
    init()

R2T_Globals.print_if_flag = args.print_if_flag
R2T_Globals.LogFile = args.logfile

# For debug purposes, set the Verbose flag (e.g True)
# print_if_flag = args.print_if_flag

print_if(args)

# tmp=rg._runProcess('ls -l /Users/ecr/fabio')

# for i in tmp:
#    print(i)

# The WBM script is a required argument and if it points to a non existing file then
# we throw an error and exit

ExistsFile = os.path.isfile(args.script)

#    sys.exit("ERROR: File {} doesn't exist".format(args.script))

# cmd = "./RGISpython.sh"

# The WBM script to read is stored in the arg.script variable...

WBMScript = args.script
WBMTemplate = args.template

# Next we declare a new WBM object which will be used to read and expose to this
# routine the settings of the WBM run specified in the command line argument

if ExistsFile:
    wbm_run = rg.wbm(WBMScript)
    print_if('Loading environment from WBM script {}'.format(WBMScript))
else:
    wbm_run = rg.wbm(WBMScript, WBMTemplate)
    print_if('From template {}, generating new environment script for WBM: {}'.format(WBMTemplate, WBMScript))

# WBMScript="/asrc/ecr/ariel/NEWS/Runs/Scripts/WBM_TP2M_925_gfdl45.sh"


# And now we read the WBM environment with the method WBMenvironment of the wbm class
# specifying the WBM script passed through the command line
#
# The WBM environment and the list of outputs are loaded into python dictionaries:
#    environment   in    EnvDict
#    outputs     in    OutDict
#
# The method WBMenvironment takes a second optional argument which initializes the
# PROJECTDIR entry of the run, and can be used for test purposes on a copy of the
# WBM output directory structure.
#
# A third optional boolean argument (splitOutput), allows to use the previous argument to save
# the outputs of this routines into a different directory structure, while still reading
# the WBM outputs form the original location defined by the WBM script.
#
# In the EnvDict dictionary the path for the WBM results root is stored in the entry
# PROJECTDIR, whereas the root for the output of this routine is stored in the entry
# NEWSLINKDIR

if args.outputFolder != '':
    #    wbm_run.WBMenvironment("/asrc/ecr/fabio/NEWS/test" ,True)
    wbm_run.WBMenvironment(args.outputFolder, True)
else:
    wbm_run.WBMenvironment()

if 'FILETYPE' not in wbm_run.EnvDict.keys():
    if 'TECH' in wbm_run.EnvDict.keys():
        wbm_run.EnvDict['RUN'] = wbm_run.EnvDict['GCM'] + '_' + wbm_run.EnvDict['RCP'] + '_' + wbm_run.EnvDict['TECH']

R2T_Globals.ProjectBase = wbm_run.EnvDict['NEWSLINKDIR']
R2T_Globals.ProjectHome = wbm_run.EnvDict['PROJECTDIR']

print_if('Entering the requested task: {}'.format(args.task))
eval(args.task)()

