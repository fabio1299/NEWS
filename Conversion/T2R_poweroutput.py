"""
T2R_poweroutput.py
"""

from . import R2T_Globals
from R2T_Globals import *
from R2T_readinputfiles import getCoordID,loadCoordID, \
                            getCoordStr,loadCombID
import subprocess as sp

import numpy as np
import pandas as pd
import time
from datetime import date, timedelta as td
import os.path
import sys


if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

def CheckZip(file):
    if os.path.isfile(file):
        out=file
    elif os.path.isfile(file + '.gz'):
        out = file + '.gz'
    else:
        raise Exception('File {} not found'.format(file))
    return out

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def T2R_Overlay(point,grid,out_file=''):

    #print_if('Sampling TP2M results with Power Plants locations')

    data1=""
    cmd=R2T_Globals.Dir2Ghaas + '/bin/pntGridSampling' + \
        ' -s ' + \
        point + \
        ' -m' + ' attrib ' + \
        grid + \
        ' -' + ' | ' + R2T_Globals.Dir2Ghaas + '/bin/rgis2table' + ' -a' + ' DBItems -'
    if out_file=='':
        proc = sp.Popen(cmd, stdout = sp.PIPE, shell=True)
        data1=StringIO(bytearray(proc.stdout.read()).decode("utf-8"))
        df=pd.read_csv(data1, sep='\t')
    else:
        #print_if('  ... and saving TXT file')
        cmd=cmd + ' > ' + out_file
        sp.call(cmd,shell=True)
        df = pd.read_csv(out_file, sep='\t')
    return df


def T2R_CalcAAC(scenario,powerplants,variable,start_yr,end_yr,base_dir,out_dir=''):

    # define output dir
    if out_dir == '':
        out_dir = base_dir + '/TP2M2ReEDS/TXT'
    else:
        out_dir = out_dir + '/TP2M2ReEDS/TXT'
    if sys.version_info[0] < 3:
        mkdir_p(out_dir)
    else:
        os.makedirs(out_dir, exist_ok=True)
    # and set the access rights to the folder, but only if the
    # script is being run with the same uid as that of the
    # folder owner (otherwise we get an error)
    if os.stat(out_dir).st_uid == os.getuid():
        os.chmod(out_dir, 0o775)

    out_dir = out_dir.replace('/TXT','')

    #print(scenario)
    MODEL, RCP, REEDS, VERSION = scenario.split("^")
# To speed up the process add row index to the total number of days in all the year
    Plants_AAC = pd.DataFrame()
    Plants_AAC['Comb_ID'] = powerplants['Comb_ID']
    Plants_Cap = pd.DataFrame()
    Plants_Cap['Comb_ID'] = powerplants['Comb_ID']
#    file1 = "../{0}/{1}/{2}/{3}/TP2M2ReEDS/TXT/{4}1_dTS{5}.txt";
#    file2 = "../{0}/{1}/{2}/{3}/TP2M2ReEDS/TXT/{4}2_dTS{5}.txt";
#    file3 = "../{0}/{1}/{2}/{3}/TP2M2ReEDS/TXT/{4}3_dTS{5}.txt";
#    file4 = "../{0}/{1}/{2}/{3}/TP2M2ReEDS/TXT/{4}4_dTS{5}.txt"
    All_Data2 = pd.DataFrame()
    for yr in range(start_yr, end_yr + 1):
        print_if('Working on year {}. Sampling Power Plant layer: '.format(yr) , ending='') #, flush=True)
#        input_file1 = file1.format(MODEL, RCP, REEDS, VERSION, variable, yr)
#        input_file2 = file2.format(MODEL, RCP, REEDS, VERSION, variable, yr)
#        input_file3 = file3.format(MODEL, RCP, REEDS, VERSION, variable, yr)
#        input_file4 = file4.format(MODEL, RCP, REEDS, VERSION, variable, yr)
        data=list()
        All_Data = pd.DataFrame()
        for sampling in range(1,5):
            print_if(sampling, ending='') #, flush=True)
            in_txt=pd.DataFrame()
            out_file = out_dir + ("/TXT/%s%s_dTS%s.txt" % (variable, sampling, yr))
            if os.path.exists(out_file):
                in_txt=pd.read_csv(out_file,sep="\t", header=0, index_col="Name")
            else:
                if (yr % 2) == 0:
                    sample_yr = yr
                else:
                    sample_yr = yr - 1
                PP_file=base_dir+("/ReEDS2TP2M/TP2Minput/PP_%s_%s.gdbp" % (sampling,sample_yr))
                if os.path.exists(PP_file):
                    VariablePath = base_dir + (
                    "/RGISresults/%s_%s_%s_%s/USA/%s%s/Pristine/Static/Daily" % (MODEL, RCP, REEDS, VERSION, variable, sampling))

                    VariableFile = CheckZip(VariablePath + ("/USA_%s%s_Pristine_Static_dTS%s.gdbc" % (variable, sampling, yr)))
                    out_file=out_dir + ("/TXT/%s%s_dTS%s.txt" % (variable, sampling, yr))
                    in_txt =T2R_Overlay(PP_file,VariableFile,out_file)
            data.append(in_txt)
        print_if(" ")

        #        data1 = pd.read_csv(input_file1, sep="\t", header=0, index_col="Name") if os.path.exists(
#            input_file1) else pd.DataFrame()
#        data2 = pd.read_csv(input_file2, sep="\t", header=0, index_col="Name") if os.path.exists(
#            input_file2) else pd.DataFrame()
#        data3 = pd.read_csv(input_file3, sep="\t", header=0, index_col="Name") if os.path.exists(
#            input_file3) else pd.DataFrame()
#        data4 = pd.read_csv(input_file4, sep="\t", header=0, index_col="Name") if os.path.exists(
#            input_file4) else pd.DataFrame()
        All_Data = pd.concat(data, axis=0) # [All_Data, data1, data2, data3, data4], axis=0)
        All_Data['Comb_ID'] = All_Data.PlantCode.astype(int).astype(str) + '_' + All_Data.Fuel.astype(int).astype(
            str) + '_' + All_Data.Cooling.astype(int).astype(str)
        All_Data = All_Data.set_index(All_Data.Comb_ID)
        d1 = date(yr, 1, 1)
        d2 = date(yr, 12, 31)
        delta = d2 - d1
        #if yr == 2032:
        #    print('pippo')
        for i in range(delta.days + 1):
            b = str(d1 + td(days=i))
            Plants_AAC[b] = Plants_AAC.Comb_ID.map(All_Data[b])
            ev_yr = yr - 1 if (yr % 2 != 0) else yr
            Plants_Cap[b] = Plants_Cap.Comb_ID.map(powerplants[('NP_%s' % ev_yr)])
    # Following 2 lines added to save the comulative AAC files for all power plants
    Plants_AAC_file=out_dir + ("/TXT/AAC_%s_%s_%s_%s.csv" % (MODEL, RCP, REEDS, VERSION))
    Plants_AAC.to_csv(Plants_AAC_file)  #### THESE FILES ARE REQUIRED FOR ANALYSES AND NEED TO BE CREATED.

    ### delete any small nameplates
    del Plants_Cap['Comb_ID']
    Plants_Cap[Plants_Cap <= 0.1] = 0.0
    Plants_Cap['Comb_ID'] = Plants_Cap.index

    Plants_AAC['RelKey'] = Plants_AAC.Comb_ID.map(powerplants.RelKey)
    Plants_Cap['RelKey'] = Plants_Cap.Comb_ID.map(powerplants.RelKey)

    ### CALCULATE DAILY AAC
    print_if('Calculating daily AAC')
    Reg_AAC = Plants_AAC.groupby('RelKey').sum() / Plants_Cap.groupby('RelKey').sum()

    #### USE MASK FOR SEASONS ####
    print_if('Applying Seasonal Masks on year: ',ending='')
    Reg_AAC = Reg_AAC.T
    Reg_AAC2 = pd.DataFrame()
    for yr in range(start_yr, end_yr + 1, 2):
        yr2 = yr + 1
        print_if(' {}'.format(yr),ending='')
        # winter
        mask = (Reg_AAC.index >= ('%s-01-01' % yr)) & (Reg_AAC.index < ('%s-03-01' % yr))
        winter1 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-11-01' % yr)) & (Reg_AAC.index <= ('%s-12-31' % yr))
        winter2 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-01-01' % yr2)) & (Reg_AAC.index < ('%s-03-01' % yr2))
        winter3 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-11-01' % yr2)) & (Reg_AAC.index <= ('%s-12-31' % yr2))
        winter4 = Reg_AAC.loc[mask]
        winter = pd.concat([winter1, winter2, winter3, winter4])
        # spring
        mask = (Reg_AAC.index >= ('%s-03-01' % yr)) & (Reg_AAC.index < ('%s-06-01' % yr))
        spring1 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-03-01' % yr2)) & (Reg_AAC.index < ('%s-06-01' % yr2))
        spring2 = Reg_AAC.loc[mask]
        spring = pd.concat([spring1, spring2])
        # summer
        mask = (Reg_AAC.index >= ('%s-06-01' % yr)) & (Reg_AAC.index < ('%s-09-01' % yr))
        summer1 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-06-01' % yr2)) & (Reg_AAC.index < ('%s-09-01' % yr2))
        summer2 = Reg_AAC.loc[mask]
        summer = pd.concat([summer1, summer2])
        # fall
        mask = (Reg_AAC.index >= ('%s-09-01' % yr)) & (Reg_AAC.index < ('%s-11-01' % yr))
        fall1 = Reg_AAC.loc[mask]
        mask = (Reg_AAC.index >= ('%s-09-01' % yr2)) & (Reg_AAC.index < ('%s-11-01' % yr2))
        fall2 = Reg_AAC.loc[mask]
        fall = pd.concat([fall1, fall2])

        winter = winter.T
        spring = spring.T
        summer = summer.T
        fall = fall.T
        #winter_aac={}
        #spring_aac={}
        #summer_aac={}
        #fall_aac={}
        #for TYPE in ['min','mean']:
        winter_aac = pd.DataFrame()
        spring_aac = pd.DataFrame()
        summer_aac = pd.DataFrame()
        fall_aac = pd.DataFrame()
        winter_aac['AAC_min'] = winter.min(axis=1)
        spring_aac['AAC_min'] = spring.min(axis=1)
        summer_aac['AAC_min'] = summer.min(axis=1)
        fall_aac['AAC_min'] = fall.min(axis=1)
        winter_aac['AAC_mean'] = winter.mean(axis=1)
        spring_aac['AAC_mean'] = spring.mean(axis=1)
        summer_aac['AAC_mean'] = summer.mean(axis=1)
        fall_aac['AAC_mean'] = fall.mean(axis=1)
        winter_aac['Year'] = np.array(yr)
        winter_aac['Season'] = np.array('Winter')
        spring_aac['Year'] = np.array(yr)
        spring_aac['Season'] = np.array('Spring')
        summer_aac['Year'] = np.array(yr)
        summer_aac['Season'] = np.array('Summer')
        fall_aac['Year'] = np.array(yr)
        fall_aac['Season'] = np.array('Fall')

        #winter = winter.T
        #winter_aac = pd.DataFrame()
        #winter_aac['AAC'] = winter.mean(axis=1)
        #winter_aac['Year'] = np.array(yr)
        #winter_aac['Season'] = np.array('Winter')
        #spring = spring.T
        #spring_aac = pd.DataFrame()
        #spring_aac['AAC'] = spring.mean(axis=1)
        #spring_aac['Year'] = np.array(yr)
        #spring_aac['Season'] = np.array('Spring')
        #summer = summer.T
        #summer_aac = pd.DataFrame()
        #summer_aac['AAC'] = summer.mean(axis=1)
        #summer_aac['Year'] = np.array(yr)
        #summer_aac['Season'] = np.array('Summer')
        #fall = fall.T
        #fall_aac = pd.DataFrame()
        #fall_aac['AAC'] = fall.mean(axis=1)
        #fall_aac['Year'] = np.array(yr)
        #fall_aac['Season'] = np.array('Fall')

        Reg_AAC2 = pd.concat([Reg_AAC2, winter_aac, fall_aac, summer_aac, spring_aac])
    print_if(" ")
    for TYPE in ['min','mean']:
        Reg_AAC2['AAC_' + TYPE] = np.where(Reg_AAC2['AAC_' + TYPE] > 1.0, 1.0,
                            Reg_AAC2['AAC_' + TYPE])  # getting rid of decimal errors but
                                                      # should also check to make sure that
                                                      # nameplate is not incorrectly lower than AAC

    ## CONVERT TECH NAMES

    print_if("Adjusting tech names to ReEDS standard")
    Reg_AAC2['CombID'] = Reg_AAC2.index
#    code_hold = pd.DataFrame(Reg_AAC2.CombID.str.split('_', 2).tolist(), columns=['BA', 'TECH', 'Cooling'])
#    Reg_AAC2['BA'] = np.array(code_hold.BA)
#    Reg_AAC2['TECH'] = np.array(code_hold.TECH)
#    Reg_AAC2['Cooling'] = np.array(code_hold.Cooling)
    Reg_AAC2['BA'] = Reg_AAC2['CombID'].apply(lambda x: x.split('_')[0]) # .astype(str).astype(int)
    Reg_AAC2['TECH'] = Reg_AAC2['CombID'].apply(lambda x: x.split('_')[1]) # .astype(str).astype(int)
    Reg_AAC2['Cooling'] = Reg_AAC2['CombID'].apply(lambda x: x.split('_')[2]) # .astype(str).astype(int)
    del Reg_AAC2['CombID']



    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '1', 'Biopower', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '2', 'Coal', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '3', 'gas-CC', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '4', 'Nuclear', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '5', 'o-g-s', Reg_AAC2.TECH)
    # Reg_AAC2.TECH=np.where(Reg_AAC2.TECH=='6', '', Reg_AAC2.TECH); # does not exist in new version of ReEDS outputs
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '7', 'coal-IGCC', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '8', 'Coal-CCS', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '9', 'gas-CC-CCS', Reg_AAC2.TECH)
    Reg_AAC2.TECH = np.where(Reg_AAC2.TECH == '10', 'gas-CT', Reg_AAC2.TECH)

    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '1', 'OT', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '2', 'RC', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '3', 'DC', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '4', 'OT', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '5', 'RC', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '6', 'DC', Reg_AAC2.Cooling)
    Reg_AAC2.Cooling = np.where(Reg_AAC2.Cooling == '8', 'NONE', Reg_AAC2.Cooling)

    Reg_AAC2 = Reg_AAC2[Reg_AAC2.TECH != 6]  # getting rid of potential 6s - but should not exist
    Reg_AAC2 = Reg_AAC2.reset_index()
    del Reg_AAC2['RelKey']

    #'/asrc/ecr/NEWS/configurations/Link/tmp'
    out_file=out_dir + '/AAC_%s' % scenario.replace('^','_')
    cols=['Year','Season','BA','TECH','Cooling']
    for TYPE in ['min','mean']:
        outDF= Reg_AAC2[Reg_AAC2['AAC_' + TYPE] <= 1]  # getting rid of NANs --- should only happen for very very small plants
        fileout=out_file + '_' + TYPE + '.csv'
        print_if("Saving final {} table to file {}".format(TYPE,fileout))
        outDF[['AAC_'+TYPE] + cols].to_csv(fileout,header=['AAC', 'Year', 'Season', 'BA', 'TECH', 'Cooling'])


if __name__ == "__main__":

    R2T_Globals.init()
    R2T_Globals.print_if_flag=True

    start_time = time.time()
#    print("--- %s seconds ---" % (time.time() - start_time))

    start_yr = 2010
    end_yr = 2050
#    ST_YR = 2010
#    END_YR = 2050
    #SEASONS = ["Winter", "Spring", "Summer", "Fall"]
    variable = "poweroutputtotal"

    MODEL = "hadgem2-es"
    RCP = "rcp8p5"
    REEDS = "bau"
    VERSION = "v000"

    base_dir=("/asrc/ecr/NEWS/LoopingPaper/%s/%s/%s/%s" % (MODEL, RCP, REEDS, VERSION))

    #pp_f = base_dir + "/ReEDS2TP2M/TP2Minput/R2Tsave/capoutput.csv"
    pp_f = base_dir + "/ReEDS2TP2M/R2Tsave/capoutput.csv"

    powerplants = pd.read_csv(pp_f, header=0)
    powerplants['Comb_ID'] = powerplants.PlantCode.astype(int).astype(str) + \
                             '_' + powerplants.Fuel.astype(int).astype(str) + \
                             '_' + powerplants.Cooling.astype(int).astype(str)
    powerplants = powerplants.set_index(powerplants.Comb_ID)

    for scenario in [("%s^%s^%s^%s" % (MODEL, RCP, REEDS, VERSION))]:
        T2R_CalcAAC(scenario,powerplants,variable,start_yr,end_yr,base_dir,'/asrc/ecr/NEWS/configurations/Link/tmp')

    print("--- %s seconds ---" % (time.time() - start_time))
