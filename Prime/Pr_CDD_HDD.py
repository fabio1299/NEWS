#!/usr/bin/python

# This is the program that calculates the Cooling and Heating Degree Days
# for a given year.
# The routine can be run as a standalone program from command line or it can
# be included in another python program.
#
# STANDALONE
# As a standalone program the program has the following options:
#        -t --temperature    The name of a gdbc file to serve as temperature input
#        -z --zones            The raster of the zones over which the CDD and HDD
#                            are calculated
#        -d --degree            The threshold used to define cooling vs. heating
#                            (default to 18.33333 Celsius)
#        -V --verbose
#
# Output is in tabular format (CSV) to standard output
#
#
# INCLUDE
# As and include module the program exposes the function:
#        cdd_hdd_year(rgGrid,pca,DegreeThreshold,verbose=False)
# where
#        rgGrid                Grid object with the temperature data
#        pca                 Mask of the PCAs
#        DegreeThreshold        Value in degrees Celsius that defines cooling and heating
#                            degrees
#        verbose                True/False (default to False)
#
# returns a dictionary of dictionaries (OutDict) structured as:
#        OutDict[Season]['cdd' | 'hdd'][zone][value]

import os
import numpy as np
import rgis as rg
import argparse
from argparse import RawTextHelpFormatter
import calendar
from R2T_Globals import *


# The following is a small routine that can help with debugging

#def print_if(value):
#    if print_if_flag:
#        print(value)
#        sys.stdout.flush()

# This is the main function (the only one used when including this file)
# It calculates the CDD and HDD given:
#        rgGrid    -    Grid object with the temperature data
#        pca     -    Mask of the PCAs
#        DegreeThreshold    -    Value in degrees Celsius that defines cooling and heating
#                            degrees

def cdd_hdd_year(rgGrid, pca, DegreeThreshold, verbose=False):

    if __name__ != "__main__":
        global print_if_flag
        print_if_flag = verbose

    # Get the year from the rgis Grid object
    year = rgGrid.Year

    # Define the seasons dictionary
    Seasons = {'spring': [('%s-03-01' % year), ('%s-05-31' % year)],
               'summer': [('%s-06-01' % year), ('%s-08-31' % year)],
               'fall': [('%s-09-01' % year), ('%s-10-31' % year)],
               'winter': [('%s-01-01' % year), ('%s-02-28' % year),
                          ('%s-11-01' % year), ('%s-12-31' % year)]}
    if calendar.isleap(int(year)):
        Seasons['winter'][1] = ('%s-02-29' % year)

    ValueTypes = ['cdd', 'hdd']

    #    print_if(Seasons)

    # We check that the pca grid is compatible with the data grid and we reshape it
    # to match <-NOT SURE WE NEED TO RESHAPE, UNLESS WE NEED TO DISPLAY
    if pca.shape != (rgGrid.nRows, rgGrid.nCols):
        if len(pca) == (rgGrid.nRows * rgGrid.nCols):
            pca.shape = (rgGrid.nRows, rgGrid.nCols)
        else:
            raise Exception('PCA data not compatible with data grid. {} vs {} elements'.format(len(pca), (
            rgGrid.nRows * rgGrid.nCols)))


            # print(rgGrid.Layers['ID'], rgGrid.Data[rgGrid.Layers['ID'],0,0])

            # for Layer in rgGrid.Layers['ID']:
            #        i = Layer # .ID
            #        print('Day {}: Min {}, Max {}, Avg {}, SDev {}'.format(i,np.nanmin(rgGrid.Data[i,:,:]),np.nanmax(rgGrid.Data[i,:,:]),np.nanmean(rgGrid.Data[i,:,:]),np.nanstd(rgGrid.Data[i,:,:])))

    pcas = np.unique(pca[pca != 0])
    #print_if('Unique values in zones {}'.format(pcas))
    # Define output structure we want to pre-allocate the space for all the
    # results to speed up the process:
    #    The total number of rows in the output DataFrame is equal to the number of
    #    regions (e.g. PCA) multiplied by 4 seasons and by 2 value points (e.g. CDD & HDD)

    #    numberOfRows=int(len(pcas)*8)

    #    numberOfRows=int(len(pcas))
    #    OutCols=[str(year)] # ['BA',str(year)]
    #    TempOut=pd.DataFrame(index=np.arange(0 ,numberOfRows),columns=OutCols)
    #    print_if('Empty TempOut: {}'.format(TempOut))

    #    OutValues={}
    OutDict = {}  # ,'Season':{'Fall':{'CDD':,'Spring','Summer','Winter'],'HDD-CDD':[,

    #print_if('Initial OutDict: {}'.format(OutDict))

    for Season in Seasons:
        OutDict[Season] = {}
        for ValueType in ValueTypes:
            OutDict[Season][ValueType] = []  # pd.DataFrame(index=np.arange(0 ,numberOfRows),columns=OutCols) # TempOut
            ##    dfOuti=0
            #print_if(dfOut)

    rgGrid.Layers = pd.merge(rgGrid.Layers,
                             pd.DataFrame(index=rgGrid.Layers.index,columns=pcas).fillna(0),
                             how='left',
                             left_index=True,
                             right_index=True)

    #pd.concat([pd.DataFrame(columns=pcas), rgGrid.Layers])

    for Day in rgGrid.Layers['ID']:
        #print(Day, type(Day))
        tmp = rgGrid.Data[Day, :, :]
        RowID = (rgGrid.Layers.ID == Day)
        for i in pcas:
            #rgGrid.Layers[i]=np.nanmean(rgGrid.Data[rgGrid.Layers.index,:,:][:,pca == i]) - 18.33333
            rgGrid.Layers.loc[RowID, i] = np.nanmean(tmp[pca == i]) - DegreeThreshold

            #print_if(rgGrid.Layers)

    for Season in Seasons:
        SeasonSubset = rgGrid.Layers[Seasons[Season][0]:Seasons[Season][1]]
        if Season == 'winter':
            SeasonSubset = SeasonSubset.append(rgGrid.Layers[Seasons[Season][2]:Seasons[Season][3]])
        # print_if('Season: {} - Number of Days: {}'.format(Season,len(SeasonSubset.index)))
        for i in pcas:
            CDD = SeasonSubset[SeasonSubset[i] > 0][i].sum()  # -457.7582893][i].sum() ## <-- REMEMBER TO TURN IT BACK TO 0
            HDD = abs(SeasonSubset[SeasonSubset[i] < 0][i].sum())  # -457.7582893][i].sum() ## <-- REMEMBER TO TURN IT BACK TO 0
            OutDict[Season]['cdd'].append(CDD)
            OutDict[Season]['hdd'].append(HDD)

    return OutDict


def cdd_hdd(GridPath, GridFile,
            start_needed, end_needed,
            pca, w_and_p_num,
            year_of_interest_start, year_of_interest_end, year_of_interest_step,
            scenario, outFile, verbose=False, DegreeThreshold=18.33333):

    if __name__ != "__main__":
        global print_if_flag
        print_if_flag = verbose

    # Initialize the list of Seasons and the list of ValueTypes
    Seasons = ['spring', 'summer', 'fall', 'winter']
    ValueTypes = ['cdd', 'hdd']


    # Getting the unique values of the zones
    pcas = np.unique(pca[pca != 0])

    print_if('Starting CDD and HDD calculations')

    # We initialize the dictionary for the storage for the output of the
    # CDD and HDD calculations
    # Knowing the size of the arrays and defining them at the beginning greatly
    # enhances the speed of the routine
    FinalCDD_HDD = {}
    for Season in Seasons:
        FinalCDD_HDD[Season] = {}
        for ValueType in ValueTypes:
            FinalCDD_HDD[Season][ValueType] = pd.DataFrame(index=range(start_needed, end_needed),
                                                           columns=pcas)  # TempOut
            #    print_if((FinalCDD_HDD))
            # Now we can loop through the Temperature files and calculate the CDD and HDD values
    for year in range(start_needed, end_needed + 1):  # start+2): #[2033,2034]: #
        print_if('Calculating HDD and CDD for year: {}'.format(year))
        inGrid = GridFile.replace('0000', str(year))

        # Here we load the Temperature data from the gdbc file of the required year
        rgGrid = rg.grid(GridPath + inGrid, True)  # clip2.gdbc' # test2000b.gdbc'
        rgGrid.Load()

        # Here we call the function that does the actual calculations the arguments
        # passed are:
        #        Temperature data loaded from the gdbc file
        #        PCA mask raster
        #        Temperature threshold between cooling and heating day
        CDD_HDD = cdd_hdd_year(rgGrid, pca, DegreeThreshold, print_if_flag)

        # The values returned from the cdd_hdd_year function are then stored in the output
        # dictionary defined above
        for Season in Seasons:
            for ValueType in ValueTypes:
                FinalCDD_HDD[Season][ValueType].loc[year] = CDD_HDD[Season][ValueType]

    #print_if((FinalCDD_HDD))
    print_if('Calculating rolling means over {} years window'.format(w_and_p_num))

    # Once we have all the results of the CDD and HDD calculation we apply the smoothing
    # over the given number of years (parameter --window on the input line)
    for Season in Seasons:
        for ValueType in ValueTypes:
            FinalCDD_HDD[Season][ValueType] = FinalCDD_HDD[Season][ValueType].rolling(window=w_and_p_num,
                                                                                      center=True).mean()
            #            FinalCDD_HDD[Season][ValueType] = pd.rolling_mean(FinalCDD_HDD[Season][ValueType],
            #                                                              window=w_and_p_num).shift(shift_number)

            # Here we rearrange the data and write out the final csv file
            # SKIP THE YEARS WITH NO DATA (EDGES OF SMOOTHING WINDOW)
            # OR FIXED NUMBERS OF YEARS? (START AT 2010 AND END AT 2050, ONLY EVERY OTHER YEAR)

    print_if('Finished rolling means calculation')
    #print_if((FinalCDD_HDD))

    print_if('Saving file')
    with open(outFile, 'w') as oFile:
        #Header = 'BA,Season,HDD-CDD,Scenario,Year,Value'
        Header = 'BA,Season,HDD-CDD,Year,Value'
        #        for year in range(start, end):
        #            Header = Header + ',' + str(year)
        oFile.write(Header + '\n')
        count = 0
        for i in pcas:
            for Season in Seasons:
                for ValueType in ValueTypes:
                    record = str(i) + ',' + Season + ',' + ValueType  # + ',' + scenario
                    for year in range(year_of_interest_start, year_of_interest_end + 1, year_of_interest_step):
                        #recout = str(count) + ',' \
                        recout = record + ',' \
                                 + str(year) + ',' \
                                 + str(FinalCDD_HDD[Season][ValueType].loc[year][i])
                        oFile.write(recout + '\n')
                        count += 1

"""
def _SortOutput(outFile, scenario):
    # Here we rearrange the data and write out the final csv file
    # SKIP THE YEARS WITH NO DATA (EDGES OF SMOOTHING WINDOW)
    # OR FIXED NUMBERS OF YEARS? (START AT 2010 AND END AT 2050, ONLY EVERY OTHER YEAR)
    with open(outFile, 'w') as oFile:
        Header = 'PCA_REG,season,cdd_hdd,scenario'
        for year in range(start, end):
            Header = Header + ',' + str(year)
        oFile.write(Header + '\n')
        for i in pcas:
            for Season in Seasons:
                for ValueType in ValueTypes:
                    record = str(i) + ',' + Season + ',' + ValueType + ',' + scenario
                    for year in range(2010, 2051, 2):
                        record = record + ',' + str(FinalCDD_HDD[Season][ValueType].loc[year][i])
                    oFile.write(record + '\n')

"""
# We first read the command line arguments and check that we have all the info needed
# to run the routine

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Calculates seasonal HDD and CDD',
                                     formatter_class=RawTextHelpFormatter)

    parser.add_argument('-t', '--temperature',
                        dest='TempGRID',
                        help="Temperature gdbc file",
                        default='./USA_airtemperature_Pristine_Static_dTS2099.gdbc')
    #        default='./test2000.gdbc')

    parser.add_argument('-z', '--zones',
                        dest='pca',
                        type=np.ndarray,
                        help="overlay zones data set (e.g. PCAs)",
                        default=None)

    parser.add_argument('-d', '--degree',
                        dest='DegreeThreshold',
                        type=np.float32,
                        help="threshold value in between Cooling and Heating degrees (default=18.33333)",
                        default=18.33333)

    parser.add_argument('-V', '--verbose',
                        dest='print_if_flag',
                        action='store_true',
                        help="Prints some debugging info",
                        default=False)

    args = parser.parse_args()

    # For debug purposes, set the Verbose flag (e.g True)
    print_if_flag = args.print_if_flag

    print_if(args)

    # Save the start time to monitor how much time it takes...
    start_time = time.time()

    # Load data from gdbc file

    rgGrid = rg.grid(args.TempGRID, True)  # clip2.gdbc' # test2000b.gdbc'
    rgGrid.Load()

    print_if("--- %s seconds for READ---" % (time.time() - start_time))

    print_if('Number of rows {}, cols {}, layers {} and bytes {}'.format(rgGrid.nRows, rgGrid.nCols, rgGrid.nLayers,
                                                                         rgGrid.nByte))

    print_if('Grid data shape {}'.format(rgGrid.Data.shape))

    if args.pca == None:
        if os.path.basename(args.TempGRID) == 'test2000.gdbc':
            pca_file = os.path.dirname(args.TempGRID) + "/clip2Mask.bil"
        else:
            pca_file = "../../Tests/PcaRegions.bil"  # #
        with open(pca_file, "rb") as ifile:
            if pca_file == "../../Tests/PcaRegions.bil":
                pca = np.fromfile(ifile, dtype=np.int16)  # 8) #16)  # read the data into numpy
                pca[pca == 32767] = False
            else:
                pca = np.fromfile(ifile, dtype=np.int8)  # 16)  # read the data into numpy
                pca[pca == 255] = False
                DegreeThreshold = -457.7582893
    else:
        pca = args.pca

    CDD_HDD = cdd_hdd_year(rgGrid, pca, args.DegreeThreshold, print_if_flag)

    pcas = np.unique(pca[pca != 0])
    for i in pcas:
        for Season in ['spring', 'summer', 'fall', 'winter']:
            for ValueType in ['cdd', 'hdd']:
                print('PCA: {}    Season: {}    CDD_HDD: {}    Value:{}'.format(i, Season, ValueType,
                                                                                CDD_HDD[Season][ValueType][i - 1]))

    print_if("--- %s seconds TOTAL ---" % (time.time() - start_time))
