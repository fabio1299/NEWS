"""
R2T_readinputfiles.py
"""
#
#


import gdxpds
import pandas as pd
import numpy as np
import os
from . import R2T_Globals
import R2T_Globals
from R2T_Globals import *

def csv_excel(filename):
    # This function returns the file type for the input data
    # Most of the routines that load the data from file can handle both excel and csv
    base, extension = os.path.splitext(filename)
    if extension == '.xls' or extension == '.xlsx':
        return 'excel'
    elif extension == '.csv':
        return 'csv'
    else:
        raise Exception('File {} not supported standard. Must be either excel (.xls or .xlsx) or csv (.csv)'.format(filename))


def Withdrawls(row):
    # Calculates the water withdrawal based on the technology info (fuel and cooling)
    # and the installed capacity
    cap = row['NP_' + str(R2T_Globals.YEAR_S)]
    f = row['Fuel']
    c = row['Cooling']
    wpMW = R2T_Globals.tech_specs[f, c][3]
    return cap * wpMW


def getTechs():
    # Splits the key of the tech_specs table and returns the technology info:
    #   fuel and cooling
    fuel=[]
    cooling=[]
    for i in R2T_Globals.tech_specs.keys():
        if i[0] not in fuel:
            fuel.append(i[0])
        if i[1] not in cooling:
            cooling.append(i[1])
    return fuel,cooling


def nextCell(row): # x,y,tocell,res=0.05):
    # Returns the x, y coordiantes of the next cell in the network
    res = R2T_Globals.Resolution
    tocell=row['ToCell']
    lon = row['Longitude']
    lat = row['Latitude']
    return findNext(tocell,lon,lat,res)

def findNext(tocell,lon,lat,res):
    if tocell == 0:
        # we have a sink, nothing to do
        return ''
    elif tocell == 1:
        x = lon + res
        y = lat
    elif tocell == 2:
        x = lon + res
        y = lat -  res
    elif tocell == 4:
        x = lon
        y = lat - res
    elif tocell == 8:
        x = lon - res
        y = lat - res
    elif tocell == 16:
        x = lon - res
        y = lat
    elif tocell == 32:
        x = lon - res
        y = lat + res
    elif tocell == 64:
        x = lon
        y = lat + res
    elif tocell == 128:
        x = lon + res
        y = lat + res
    else:
        raise Exception('Invalid ToCell code {}'.format(tocell))
    return getCoordID(x, y) # '{:8.3f}'.format(x).strip() + "_" + '{:8.3f}'.format(y).strip()

#def adjust_cooling(row):
    # NOTE THIS FUNCTION CHANGES WHEN THE RULES AND/OR THE CODES CHANGE
    # THE UNCOMMENTED VERSION BELOW IS FOR THE NEW RULES/CODES AS DEFINED
    # AFTER THE APRIL 2017 DENVER MEETING
    # THIS VERSION IS THE ONE BEFORE THE DENVER MEETING
    # Applies the cooling adjustments based on fuel type
    # Can be used with the DataFrame.apply() function
#    if (int(row['Fuel']) in [3]) and (int(row['Cooling']) != 8):
#        return int(row['Cooling']) + 3
#    else:
#        return int(row['Cooling'])

def adjust_cooling(row):
    # NOTE THIS FUNCTION CHANGES WHEN THE RULES AND/OR THE CODES CHANGE
    # ABOVE IS THE COMMENTED VERSION (BEFORE THE DENVER MEETING SETTINGS)
    # THIS VERSION IS FOR THE NEW RULES/CODES AS DEFINED AFTER THE
    # APRIL 2017 MEETING IN DENVER
    # Applies the cooling adjustments based on fuel type
    # Can be used with the DataFrame.apply() function

    # We first change all the cooling types NONE (8) to Dry (3) except for
    # fuel type gas-CT and Gas-CT-NSP (10)

    if (int(row['Fuel']) != 10) and (int(row['Cooling']) == 8):
        row['Cooling'] = 3

    # Then we add 3 to the cooling type when the technology is any of:
    # coal-IGCC (7), gas-CC (3), gas-CC-CCS (9), Gas-CC-NSP (3)

    if (int(row['Fuel']) in [3,7,9]) and (int(row['Cooling']) != 8):
        return int(row['Cooling']) + 3
    else:
        return int(row['Cooling'])

def reset_cooling(row):
    # NOTE THIS FUNCTION CHANGES WHEN THE RULES AND/OR THE CODES CHANGE
    # ABOVE IS THE CONVERSION FROM ReEDS to TP2M
    # THIS FUNCTION RESETS THE Cooling CODES TO THE ORIGINALS DEFINED
    # BY ReEDS
    #
    # Applies the cooling adjustments based on fuel type
    # Can be used with the DataFrame.apply() function

    # We first change all the cooling types NONE (8) to Dry (3) except for
    # fuel type gas-CT and Gas-CT-NSP (10)

    if (int(row['Fuel']) != 10) and (int(row['Cooling']) == 8):
        row['Cooling'] = 3

    # Then we add 3 to the cooling type when the technology is any of:
    # coal-IGCC (7), gas-CC (3), gas-CC-CCS (9), Gas-CC-NSP (3)

    if (int(row['Fuel']) in [3,7,9]) and (int(row['Cooling']) != 8):
        return int(row['Cooling']) - 3
    else:
        return int(row['Cooling'])


def getCoordStr(Coord):
    CoordStr='{:8.3f}'.format(Coord)
    return CoordStr.strip()

def getCoordID(Lon, Lat):
    return getCoordStr(Lon) + '_' + getCoordStr(Lat)

def loadCoordID(row):
    return getCoordID(row['Longitude'],row['Latitude'])

def loadCombID(row):
    PlantCode=int(row['PlantCode'])
    Fuel=int(row['Fuel'])
    Cooling=int(row['Cooling'])
    CombID = str(PlantCode) + \
             '_' + str(Fuel) + \
             '_' + str(Cooling)
    return CombID

def AssembleData(powerplants, capacity, efficiency, year, finalyear):
    # This routine reads and adjusts the power plant data from the following files:
    #   PowerPlants characteristics - includes the coordinates, lake/ocean flag etc.
    #   Capacity - power platns historical yearly installed capacity
    #   Efficiency - power platns historical yearly efficiency
    #
    # It returns 1 dataframes:
    #   1) a preliminary version of the PlantsByID (will get additional info once the HydroCells are loaded)

    if not os.path.isfile(powerplants):
        raise Exception('Input file {} not found'.format(powerplants))
    if not os.path.isfile(capacity):
        raise Exception('Input file {} not found'.format(capacity))
    if not os.path.isfile(efficiency):
        raise Exception('Input file {} not found'.format(efficiency))

    if (finalyear % 2) == 0:
        finalAdjust = 1
    else:
        finalAdjust = 2

    # The following dictionary is used to rename the columns to standard names used
    # in the rest of the routine
    PowerPlantsCols={'Index':'Index',
                     'EIA_ID':'PlantCode',
                     'NP_2010':'NP_2010',
                     'Fuel':'Fuel',
                     'Cooling':'Cooling',
                     'Eff_2010':'Eff_2010',
                     'State':'State',
                     'withdrawal':'Withdrawal',
                     'PCA':'PCA',
                     'Longitude':'Longitude',
                     'Latitude':'Latitude',
                     'LakeOcean':'LakeOcean',
                     'AltWater':'AltWater',
                     'NP_2012':'NP_2012',
                     'NP_2014': 'NP_2014',
                     'NP_2016':'NP_2016',
                     'Eff_2012':'Eff_2012',
                     'Eff_2014':'Eff_2014',
                     'Eff_2016':'Eff_2016',
                     'RetireYear':'RetireYear'
                    }

    PrimeMover_Dict = {'ST': 1, 'CC': 2}


    # Now we load the capacity and the efficiency of the power plants.
    # The file structures for these data are the same, so we can load the data using
    # the same commands in a parametrized loop (easier code maintenance)
    #
    # We store the results in a dictionary of data frames

    dfs = {}

    for param in ['NP_','Eff_']:
        if param == 'NP_':
            message = 'Loading Capacity data starting from year {} from file {}. End year in file set to {}'.\
                format(year, capacity,finalyear)
            message2 = 'Adjusting Name Plates for power plants'
            inFile=capacity
        else:
            message = 'Loading Efficiency data starting from year {} from file {}. End year in file set to {}'.\
                format(year,efficiency,finalyear)
            message2 = 'Adjusting efficiencies for power plants'
            inFile=efficiency
        print_if(message)

        cols = ['EIA_ID', 'Fuel', 'Cooling']
        cols_tmp=[]

        # The files are stored with a column for each of the 4 yearly layers used in the TP2M model
        # so for each year we need to read the data for the 4 layers.

        for y in range(year, finalyear + 1, 2):
            for i in range(1, 5):
                cols_tmp = cols_tmp + [param + str(i) + '_' + str(y) ]

        # If we have an odd end year, then we load that as our final
        # At the end of the data load, it will be renamed to represent the end year + 1
        # time step

        if finalAdjust == 2:
            for i in range(1,5):
                cols_tmp = cols_tmp + [param + str(i) + '_' + str(finalyear)]  # ,'Latitude','Longitude']

        # We merge the base column names with the sequences generated by the loops above
        cols = cols + cols_tmp

        # and we read the file
        if csv_excel(capacity) == 'csv':
            df_tmp = pd.read_csv(inFile, header=0, usecols=cols)[cols] # sep='\t',
        else:
            raise Exception('Only csv files allowed for power plants capacity and efficiency data')

        df_tmp['Fuel']=df_tmp['Fuel'].apply(lambda x: int(x))
        df_tmp['Cooling']=df_tmp['Cooling'].apply(lambda x: int(x))

        # now we add the 4 layers values into a single value for each year
        print_if(message2)
        for y in range(year, finalyear + 1, 2):
            cols = []
            for i in range(1,5):
                cols = cols + [param + str(i) + '_' + str(y)]
            df_tmp[param + str(y)] = df_tmp[cols].sum(axis=1)

        if finalAdjust == 2:
            cols = []
            for i in range(1,5):
                cols = cols + [param + str(i) + '_' + str(finalyear)]  # ,'Latitude','Longitude']
            df_tmp[param + str(finalyear)] = df_tmp[cols].sum(axis=1)

        # and we drop the temporary columns (the 4 layers columns)
        df_tmp.drop(cols_tmp, inplace=True, axis=1)

        # finaly we store the result in our dictionary
        dfs[param[:-1]] = df_tmp

    print_if('Loading Power Plants characteristics from file {}'.format(powerplants))

    cols = ['EIA_ID', 'Fuel', 'Cooling', 'PrimeMover', 'LakeOcean', 'Latitude', 'Longitude'] + ['fuel_' + str(i) for i in
                                                range(1,5)] + ['cooling_' + str(i) for i in
                                                range(1,5)]
    if csv_excel(powerplants) == 'csv':
        df_pp = pd.read_csv(powerplants, header=0, usecols=cols)[cols] # sep='\t',
    else:
        raise Exception('Only csv files allowed for power plants characteristics data')

    print_if('Adjusting fuel code for power plants')
    cols = ['fuel_' + str(i) for i in range(1,5)]
    df_pp['Fuel'] = df_pp[cols].sum(axis=1)
    df_pp['Fuel'] = df_pp['Fuel'].apply(lambda x: int(x))

    print_if('Adjusting cooling code for power plants')
    cols = ['cooling_' + str(i) for i in range(1,5)]
    df_pp['Cooling'] = df_pp[cols].sum(axis=1)
    df_pp['Cooling'] = df_pp['Cooling'].apply(lambda x: int(x))

    cols = ['fuel_' + str(i) for i in range(1, 5)] + ['cooling_' + str(i) for i in range(1,5)]
    df_pp.drop(cols, inplace=True, axis=1)

    print_if(
        'Merging Power Plant Charcteristics with Capacity and Efficiency for years {} - {}'.format(year, finalyear))
    df_tmp = pd.merge(dfs['NP'], dfs['Eff'], how='left', on=['EIA_ID', 'Fuel', 'Cooling'])

    df_tmp = pd.merge(df_pp, df_tmp, how='left', on=['EIA_ID', 'Fuel', 'Cooling'])

    df_tmp['Index'] = df_tmp.index

    print_if('Adding all columns to merged Power Plants data')
    cols = ['Index','EIA_ID', 'NP_' + str(year),
            'PrimeMover', 'Fuel',
            'Cooling', 'Eff_' + str(year),
            'State', 'Orig_lat',
            'Orig_long', 'circulating',
            'withdrawal', 'consumption',
            'blowdown', 'deltaT', 'heat sink',
            'Co2 Rate', 'water_source', 'PCA',
            'WBA ID', 'WBA Name', 'ToCell',
            'FromCell', 'Order_', 'BasinID',
            'BasinCells', 'Travel', 'CellArea',
            'CellLength', 'SubbasinArea',
            'SubbasinLength', 'Longitude',
            'Latitude', 'avg_ats',
            'LakeOcean',  'AltWater'] # 'HydroID',
    if finalAdjust == 2:
        cols = cols + ['NP_' + str(y) for y in range(year + 2, finalyear + 1, 2)] + ['NP_' + str(finalyear)]
    else:
        cols = cols + ['NP_' + str(y) for y in range(year + 2, finalyear + 1, 2)]
    if finalAdjust == 2:
        cols = cols + ['Eff_' + str(y) for y in range(year + 2, finalyear + 1, 2)] + ['Eff_' + str(finalyear)]
    else:
        cols = cols + ['Eff_' + str(y) for y in range(year + 2, finalyear + 1, 2)]
    cols = cols + ['RetireYear', 'original ratio withdrawal:summer',
                   'water Distance from coast (km)',
                   'current ratio withdrawal:summer water']
    df_tmp1 = pd.DataFrame(index=df_tmp.index, columns=cols)

    df_tmp1 = df_tmp1.fillna(0)

    print_if('Redefining PrimeMover to numeric')
    df_tmp1[df_tmp.columns] = df_tmp.replace({"PrimeMover": PrimeMover_Dict})

#    print_if('Adjusting PCA to numeric')
#    df_tmp1['PCA'] = df_tmp1['PCA'].apply(lambda x: int(x[1:]))

    print_if('Calculating Power Plant withdrawal')
    # cap * wpMW
    df_tmp1['withdrawal'] = df_tmp1.apply(lambda row: Withdrawls(row), axis=1)

    # If the final year in the Power Plants database is odd (or not in the 2 years step
    # sequence used by ReEDS, then we rename it to the next upper year (e.g. if the last
    # year in the database is 2015 we rename it 2016)
    if finalAdjust == 2:
        print_if('Renaming final power plant database year {} to {}'.format(finalyear, finalyear + 1))
        df_tmp1.rename(columns={'NP_' + str(finalyear): 'NP_' + str(finalyear + 1)}, inplace=True)
        df_tmp1.rename(columns={'Eff_' + str(finalyear): 'Eff_' + str(finalyear + 1)}, inplace=True)

    # We check which years of the analysis are already given in the Power Plants database and store the result
    # in the DBYears list (declared global so that it can be used in other functions)
    r_i=df_tmp1.columns
    for y in R2T_Globals.YRS:
        if "NP_" + str(y) in r_i:
            R2T_Globals.DBYears.append(y)
        else:
            break
    # Now we get all the plants we are interested in (only those whose fuel and cooling are included in the
    # tech_spec dictionary
    gfuels, gcooling = getTechs()
    for col in df_tmp1.columns:
        if col not in PowerPlantsCols.keys():
            df_tmp1.drop(col, axis=1, inplace=True)
    df_tmp1.rename(columns=PowerPlantsCols, inplace=True)

    df_tmp1=df_tmp1[((df_tmp1['Fuel'].isin(gfuels)) & (df_tmp1['Cooling'].isin(gcooling)))]
    df_tmp1.set_index(['Index'], drop=False, inplace=True)
    # We add a RetireYear field to the power plants and set it to the year when the initial capacity (e.g. capacity
    # of the years in the Power Plant databae) goes to 0 (and stays at zero for the rest of the year in the Power
    # Plants database). This allow them to be retired and (eventually) recycled at the beginning of each year
    EndYearInDatabase = max(R2T_Globals.DBYears)
    for i in range(0, len(R2T_Globals.DBYears)):
        Years = list(R2T_Globals.DBYears)
        curr_y = R2T_Globals.DBYears[i]
        del Years[0:i]
        cols=[]
        for y in Years:
            cols.append('NP_'+str(y))
        df_tmp1['tempSum']=df_tmp1[cols].sum(axis=1)
        df_tmp1.loc[((df_tmp1['tempSum'] == 0) & (df_tmp1['RetireYear'] == 0)) ,'RetireYear'] = curr_y
    df_tmp1.drop('tempSum', axis=1, inplace=True)

    # We set the remainig RetireYear to a large number to speed up the later search
    df_tmp1.loc[(df_tmp1['RetireYear'] == 0), 'RetireYear'] = 9999
    # Now we group by PCA, Fuel and Cooling and sum the capacities of the corresponding Power Plants
    # This will be used to create the table of the delta capacities
#    Sums=df_tmp1.groupby(['PCA','Fuel','Cooling'])[["NP_" + str(y) for y in R2T_Globals.DBYears]].sum()
#    Sums=pd.DataFrame()

    return df_tmp1 # ,Sums


def LoadDischarge(df_data, hydrocells):
    # Loads the network information required for the processing
    # NOTE: loading the data from an excel file can take as much as 30 times longer than
    #       loading it from a csv
    #
    #
    # Takes the followign input:
    #   PlantsByID dataframe created by the AssembleData function
    #   The file name fo the HydroCells data
    #
    # Returns 3 dataframes:
    #   1) Finalized PlantsByID dataframe (with water inforamtion added)
    #   2) Network data ("cells" dataframe)
    #   3) the Sums dataframe with the summary info of the installed capacity by PCA, Fuel and Cooling
    #

    CellsCols={'CellID':'CellID',
               'ToCell':'ToCell',
               'Coastal':'Coastal',
               'CellXCoord':'Longitude',
               'CellYCoord':'Latitude',
               'Avg_Sum':'AvailDisch',
               'Summer_Discharge':'TotAvgSumDisch',
               'PCA_REG':'PCA'
                }
    if not os.path.isfile(hydrocells): return
    print_if('Loading Cells Discharge from file {}'.format(hydrocells))
    if csv_excel(hydrocells) == 'excel':
        df_hydro = pd.read_excel(hydrocells, sheetname=0)
        for col in df_hydro.columns:
            if col not in CellsCols.keys():
                df_hydro.drop(col, axis=1, inplace=True)
    else:
        df_hydro = pd.read_csv(hydrocells, header=0, usecols=CellsCols.keys())

    print_if('Extracting USA and renaming the HydroCells columns')

    df_hydro=df_hydro.loc[df_hydro['PCA_REG'] != 0].rename(columns=CellsCols)
    #df_hydro = df_hydro.rename(columns=CellsCols)

    print_if('Re-indexing the HydroCell table')
#    df_hydro['Lon'] = df_hydro['Longitude'].apply(lambda x: '{:8.3f}'.format(x))
#    df_hydro['Lat'] = df_hydro['Latitude'].apply(lambda x: '{:8.3f}'.format(x))
#    df_hydro['CoordID'] = df_hydro['Lon'].apply(lambda x : x.strip()) + "_" + df_hydro['Lat'].apply(lambda x : x.strip())
    df_hydro['CoordID'] = df_hydro.apply(loadCoordID, axis=1)
    df_hydro.set_index(['CoordID'], inplace=True)
#    print_if('Saving network cells PCA code')
#    df_hydro['OrigPCA'] = df_hydro['PCA_REG']
    print_if('Re-indexing Power Plants data')
#    df_data['Lon'] = df_data['Longitude'].apply(lambda x: '{:8.3f}'.format(x))
#    df_data['Lat'] = df_data['Latitude'].apply(lambda x: '{:8.3f}'.format(x))
#    df_data['CoordID'] = df_data['Lon'].apply(lambda x : x.strip()) + "_" + df_data['Lat'].apply(lambda x : x.strip())
    df_data['CoordID'] = df_data.apply(loadCoordID, axis=1)
    df_data.set_index(['CoordID'], inplace=True)
    print_if('Adding reference to network Cell ID and PCA in the Power Plant table')
#    df_data = df_data.merge(df_hydro[['CellID','PCA']], how='left', left_index=True, right_index=True)
    df_data['PCA']= df_hydro['PCA']
    df_data['CellID']= df_hydro['CellID']
#    df_data.reset_index(inplace=True)
    print_if('Calculating Power Plants withdrawal by network cell')
    df_used = df_data.loc[:, ['Withdrawal','Longitude','Latitude','Cooling']]
    df_used = df_used.merge(df_hydro[['ToCell']], how='left', left_index=True, right_index=True)
    df_used.reset_index(inplace=True)
#    df_data.set_index(['CoordID'], inplace=True)
    df_used['Occupied'] = 1
    df_used['Locked']=df_used.apply(nextCell,axis=1)
    df_used.loc[~df_used['Cooling'].isin([1,4]),'Locked']=''
    aggregate={'Withdrawal' : {'Withdrawal' : 'sum'},
               'Occupied' : {'Occupied' : 'sum'},
               'Locked' : {'Locked' : 'max'}}
    df_used1 = df_used.groupby(['CoordID']).agg(aggregate)
    df_used1.columns = df_used1.columns.droplevel()
#    print_if('Re-indexing the HydroCell table')
#    df_hydro.reset_index(drop=True, inplace=True)
#    df_hydro.set_index(['CellID'], inplace=True, drop=False)
#    print_if('Re-indexing the Power Plant table')
#    df_data.reset_index(drop=True, inplace=True)
#    df_data.set_index(['CellID'], inplace=True, drop=False)
    print_if('Removing Power Plants withdrawals from Total Average Summer Discharge')
    df_hydro = df_hydro.merge(df_used1[['Withdrawal','Occupied','Locked']], how='left', left_index=True, right_index=True).fillna(0)
    df_hydro['AvailDisch'] = df_hydro['TotAvgSumDisch'] * .3 - df_hydro['Withdrawal']
    print_if('Marking network cells as Occupied where power plants already exist')
    df_hydro['Occupied']=df_hydro['Occupied'].astype(int)
    print_if('Adjusting locked status for network cells downstream of power plants with cooling Once-Through')
    df_hydro.loc[df_hydro['Locked']==0,'Locked']=''

#    df_hydro['Occupied'] = df_hydro['Count']
    print_if('Adding Available Discharge info to power plants table')
    df_data = df_data.merge(df_hydro[['AvailDisch']], how='left', left_index=True, right_index=True)
#    df_data['AvailDisch'] = df_data['Avg_Sum']
#    for hd in df_used.index.values:
#        # Available discharge is only 30% of what's in the river, so the available water
#        # at any given cell is 30% of the discharge minus what is already withdrawn by
#        # the power plants that are in that cell
#        df_hydro.loc[hd, 'Avg_Sum'] = df_hydro.loc[hd, 'Avg_Sum'] * .3 - df_used.loc[hd, 'Withdrawal']
#        df_data.loc[hd, 'AvailDisch'] = df_hydro.loc[hd, 'Avg_Sum']
#        PCApp = df_data.loc[hd, 'PCA']
#        if isinstance(PCApp, pd.Series):
#            PCApp = PCApp.iloc[0]
#        if df_hydro.loc[hd, 'PCA_REG'] != PCApp:
#            df_hydro.loc[hd, 'PCA_REG'] = PCApp
#        df_hydro.loc[hd, 'Occupied'] = df_used.loc[hd, 'Count']

#    df_data.drop(['Lon', 'Lat'], axis=1, inplace=True)
    df_data.reset_index(inplace=True)
    df_data.set_index('Index',drop=False,inplace=True)

#    df_hydro.drop(['Lon', 'Lat', 'Withdrawal'], axis=1, inplace=True)
    df_hydro.drop(['Withdrawal'], axis=1, inplace=True)
    df_hydro.reset_index(inplace=True)
    df_hydro.set_index(['CellID'],drop=False,inplace=True)

    # Now we group by PCA, Fuel and Cooling and sum the capacities of the corresponding Power Plants
    # This will be used to create the table of the delta capacities
    Sums=df_data.groupby(['PCA','Fuel','Cooling'])[["NP_" + str(y) for y in R2T_Globals.DBYears]].sum()


    return df_data.sort_index(), df_hydro, Sums # .loc[df_hydro['PCA_REG'] != 0].rename(columns=CellsCols)

def read_cap_deltas(capacities_file, sums, file_type):
    # This routine reads the ReEDS output file and creates the table of the
    # capacities deltas for every other year (2010 to 2050) for each PCA, Fuel, Cooling combination
    #
    # Input parameters are:
    #   file name (if from csv) or data buffer (if from excel) of the ReEDS output
    #   summary dataframe of the installed capacity (from Power Plant database)
    #
    # Returns one dataframe:
    #   capdeltas - for each combiantion of PCA, Fuel and Cooling gives the yearly change
    #               in installed capacity


    # The following dictionary is used to rename the columns to standard names used
    # in the rest of the routine
    CapInputCols={'Tech':'Fuel','Cooltech':'Cooling','PCA':'PCA','Year':'Year','MW':'Capacity'}
    # Now we read the ReEDS capacity output into a pandas dataframe
    # Depending on the type of file we get we do a different type of read
    if isinstance(capacities_file, str):
        print_if('Reading capacities from file {}'.format(capacities_file))
        if file_type == '.csv':
            reeds = pd.read_csv(capacities_file, sep=',', usecols=CapInputCols.keys()).rename(columns=CapInputCols)
        else:
            CapInputCols = {'bigQ': 'Fuel', 'ct': 'Cooling', 'n': 'PCA', 'allyears': 'Year', 'Value': 'Capacity'}
            reeds =gdxpds.to_dataframe(capacities_file,symbol_name='CONVqctnallyears',old_interface=False).rename(columns=CapInputCols)
    else:
        reeds=capacities_file.parse('CONVqctn')
        for col in reeds.columns:
            if col not in CapInputCols.keys():
                reeds.drop(col,axis=1,inplace=True)
        reeds.rename(columns=CapInputCols, inplace=True)

    # We extract the technology types that we want to include in the analysis
    # These are the technology types included inte tech_spec dictionary (in module Global.py)
    # We store these values in the list techs (without duplicates)
    techs=R2T_Globals.tech_specs.keys() #,dump=getTechs()

    # Then we cut the 'p' from the PCA code (ReEDS writes the PCA code preceeded by a 'p'
    # we want it to be numeric as we us it to generate the relkey)
    reeds['PCA']=reeds['PCA'].apply(lambda x: int(x[1:])).astype(str).astype(int)

    # And we take the PCAs we are interested in
    reeds=reeds.loc[reeds['PCA'].isin(R2T_Globals.PCAs)]

    # And make sure that the Year is int
    reeds['Year']=reeds['Year'].apply(lambda x: int(x))

    # Convert the dictionaries to int
    fuels2 = {}
    for key, value in R2T_Globals.fuels.items():
        fuels2[key.lower()] = int(value)
    cooling2 = {}
    for key, value in R2T_Globals.cooling.items():
        cooling2[key.lower()] = int(value)

    reeds['Fuel']=reeds['Fuel'].apply(lambda x: x.lower())
    reeds['Cooling']=reeds['Cooling'].apply(lambda x: x.lower())

    # Now we use the fuels2 dictionary (again in R2T_Globals.py) to replace the ReEDS technology alpha-codes
    # with the numeric codes we use
    reeds['Fuel'].replace(fuels2, inplace=True)

    # We do the same the cooling2 dictionary (again in R2T_Globals.py) to replace the ReEDS cooling alpha-codes
    # with the numeric codes we use
    reeds['Cooling'].replace(cooling2, inplace=True)

    # Here we do a few adjustments:
    #   current:
    #       if fuel (Tech) is 3 and coooling (Cooltech) is 1,2, or 3 then we add 3 to the cooling code
    # CHANGE THE FUNCTIONS CALLED TO DO THE ADJUSTMENT (adjust_cooling) TO IMPLEMENT DIFFERENT RULES

    reeds['Cooling']=reeds.apply(adjust_cooling,axis=1)

    # Now that we have all the numeric codes we can extract only the records that have the technology code (Tech)
    # included in the list techs that we created at the beginning of this function
    reeds.set_index(['Fuel','Cooling'],inplace=True)
    reeds=reeds.loc[reeds.index.isin(techs)]
    reeds.reset_index(inplace=True)

    # NO NEED TO DEAL WITH MERGING Tech=3,Cooling=8 WITH Tech=6,Cooling=8
    # OTHERWISE WE WOULD JUST CHANGE THE VALUES HERE, BEFORE DOING THE PIVOT TABLE
    #   if fuel is 6 and coooling (Cooltech) is 8 then we change the fuel type to 3
    #   (effectively merging Tech 6, Cool 8 with Tech 3, Cool 8)
    reeds.loc[((reeds['Fuel']==6) & (reeds['Cooling'] == 8)),'Fuel'] = 3

    # Now we pivot the dataframe so that we have our relkeys as row index, the years as columns and the
    # sums of the respective MW as values (the sum takes care of aggregating Tech 6, Cool 8 with Tech 3, Cool 8)
    Caps=reeds.pivot_table(index=['PCA','Fuel','Cooling'],columns='Year',values='Capacity',fill_value=0,aggfunc=np.sum)

    # We now drop year 2005 (if present). 2005 is used for ReEDS spinup and is not used in our conversion...
    if 2005 in Caps.columns:
        Caps.drop(2005, axis=1, inplace=True)
    if R2T_Globals.SaveCap:
        SaveCapacity(R2T_Globals.SaveDir + "ReEDS_cap.xlsx",Caps.reset_index(level=['PCA','Fuel','Cooling']))
    CapDeltaCols={}
    for y in R2T_Globals.YRS:
        CapDeltaCols[y]="NP_"+str(y)

    # We now calculate the yearly deltas (changes in capacity from year to year) including the
    # adjustment of the values we get from the Power Plants database.
    # We start by integating the indexes (PCA,Fuel,Cooling) of the power plant database with the one
    # of the ReEDS output

    # Here we convert the keys of the sums dictionary to a Pandas index
    sumIndex=sums.index # pd.MultiIndex.from_tuples(sums.keys())

    # and then we merge it the index read from the ReEDS results
    allIndex=Caps.index.union(sumIndex)

    # Now we can create an empty dataframe that has the joint index and the columns from the ReEDS results
    CapDeltas=pd.DataFrame(index=allIndex,columns=Caps.columns)

    # We populate the new dataframe with the values read from the ReEDS output
    for col in CapDeltas.columns:
        CapDeltas[col]=Caps[col]
    CapDeltas.fillna(0,inplace=True)
    CapDeltas=CapDeltas.rename(columns=CapDeltaCols)

    # First we deal with the keys of the ReEDS output that are not present in the
    # Power Plant database
    CapDeltasNoSum=CapDeltas.loc[Caps.index.difference(sums.index)]
    ColsRev=CapDeltas.columns[::-1]
    for i in range(len(ColsRev) - 1):
        CapDeltasNoSum[ColsRev[i]] = CapDeltasNoSum[ColsRev[i]] - CapDeltasNoSum[ColsRev[i+1]]

    # Then we deal with the keys of the ReEDS output that are also in the
    # Power Plant database
    CapDeltasInSum=CapDeltas.loc[sums.index]

    # Finally, scanning the table's columns in reverse order we subtract year Y-1 from year Y
    # (e.g. 2050-2048, 2048-2046 ... 2018-2016) and obtain the capacity deltas for each year
    ColsRev=[val for val in CapDeltas.columns[::-1] if int(val[3:]) not in R2T_Globals.DBYears] + ["NP_"+str(max(R2T_Globals.DBYears))]
    for i in range(len(ColsRev)-1):
        CapDeltasInSum[ColsRev[i]] = CapDeltasInSum[ColsRev[i]] - CapDeltasInSum[ColsRev[i+1]]

    # And we subtract what we already have in the Power Plant database
    #    for key in sums:
    for key in sums.index:
        for y in R2T_Globals.DBYears:
            yKey = "NP_" + str(y)
            CapDeltasInSum.loc[key, yKey] = CapDeltasInSum.loc[key, yKey] - sums.loc[key, yKey]  # sums[key][y]
    CapDeltas=CapDeltasInSum.append(CapDeltasNoSum,verify_integrity=True).sort_index()

    return CapDeltas

def read_retirements(retirements_file):
    """
    read in retirements table
    Input: file name of the retirement data (can read both csv and excel)
    Output: retirements dataframe (called retire_dict for legacy coding)
    """
#    RetireCols={'Index':'Index','PlantCode':'PlantCode',
#                'RetirementYr':'Year','CapacityRetired':'Capacity',
#                'PCA':'PCA','FuelType':'Fuel','CoolingType':'Cooling',
#                'Online':'Online'
#                }
    RetireCols={'Index' : 'Index',
                'Nameplate' : 'Capacity',
                'Online' : 'Online',
                'pca' : 'PCA',
                'RetireYear' : 'Year',
                'EIA_ID' : 'PlatCode',
                'Fuel' : 'Fuel',
                'cooling_code' : 'Cooling'
                }

    if csv_excel(retirements_file) == 'excel':
        retire_dict=pd.read_excel(retirements_file)
    else:
        retire_dict = pd.read_csv(retirements_file)
    retire_dict=retire_dict.rename(columns=RetireCols)
    if isinstance(retire_dict['PCA'][0], str):
        print_if('Adjusting retirement table PCA to numeric')
        retire_dict['PCA'] = retire_dict['PCA'].apply(lambda x: int(x[1:]))
    retire_dict.set_index(['Index'],drop=False,inplace=True)
    return retire_dict


def get_gen_input(gen_file,file_type):

    # This routine reads the ReEDS output file and creates the table of the
    # seasonal generation for every other year (2010 to 2050) for each PCA, Fuel, Cooling combination
    #
    # Input parameters are:
    #   file name (if from csv) or data buffer (if from excel) of the ReEDS output
    #
    # Returns one dataframe:
    #   gen_input - for each combiantion of PCA, Fuel and Cooling gives the monthly generation required

    # Lookup dataframes created:
    #   fuel type = fuels_lkup
    #   cooling technology = cooling_lkup
    cooling_lkup = pd.DataFrame(list(R2T_Globals.cooling.items()), columns=['Cooltech', 'Cooling'])
    cooling_lkup['Cooling'] = cooling_lkup['Cooling'].astype(int)
    fuels_lkup = pd.DataFrame(list(R2T_Globals.fuels.items()), columns=['Tech', 'Fuel'])
    fuels_lkup['Fuel'] = fuels_lkup['Fuel'].astype(int)

    # Defines the fields of interest
    InColumns=['Tech','Cooltech','PCA','Year','Timeslice','MWtimeslice']

    # Load the data from the ReEDS file (only the columns listed above)
    if isinstance(gen_file, str):
        print_if('Reading generation data from file {}'.format(gen_file))
        if file_type == '.csv':
            in_gen = pd.read_csv(gen_file, sep=",", usecols=InColumns)
        else:
            InColumns = {'bigQ': 'Tech', 'ct': 'Cooltech', 'n': 'PCA', 'allyears': 'Year', 'm': 'Timeslice', 'Value': 'MWtimeslice'}
            in_gen =gdxpds.to_dataframe(gen_file,symbol_name='CONVqctmnallm',old_interface=False).rename(columns=InColumns)
    else:
        in_gen=gen_file.parse('CONVqctmnallm')
        for col in in_gen.columns:
            if col not in InColumns:
                in_gen.drop(col, axis=1, inplace=True)

    # Merge with the various lookup tables
    in_gen = pd.merge(in_gen, fuels_lkup, on='Tech')
    in_gen = in_gen.loc[in_gen['Fuel'].isin(R2T_Globals.Fuels)]
    in_gen['PCA'] = in_gen['PCA'].str.replace('p', ' ').astype(int)
    in_gen = in_gen.loc[in_gen['PCA'].isin(R2T_Globals.PCAs)]
    in_gen = pd.merge(in_gen, R2T_Globals.time_lkup, on='Timeslice')
    in_gen = pd.merge(in_gen, cooling_lkup, on='Cooltech')
    in_gen['Year']=in_gen['Year'].apply(lambda x: int(x))

    # Convert the MWh of each Time Slice to the Total MWh generated
    # over the entire season by multiplying by the number of hours
    # in each Time Slice

    in_gen['MWseason'] = in_gen['MWtimeslice'] * in_gen['Hours']

    # Adjust the cooling technology code for the Combined Cycles
    # power plants. Refer to function adjust_cooling for further information

    in_gen['Cooling'] = in_gen.apply(adjust_cooling, axis=1)

    # If the Flag that defines the merge of Fuel Type 6 Cooling 8 with
    # Fuel Type 3 Cooling is set, then adjust the values
    if R2T_Globals.merge6_8and3_8:
        in_gen.loc[((in_gen['Fuel'] == 6) & (in_gen['Cooling'] == 8)), ['Fuel']] = 3

    # Create the RelKey (logic backwards compatible with the Access routines)
    in_gen['RelKey'] = in_gen.apply(SetRelKey,axis=1) # SetRelKey(in_gen['PCA'], in_gen['Fuel'], in_gen['Cooling'])

    # Create a combined season + year key for the pivot table (avoids multi-index)
    in_gen['Season']=in_gen['Season'] + in_gen['Year'].astype(str)

    # Create the generation input table by creating a pivot table of the input data
    gen_input = pd.pivot_table(in_gen, values=['MWseason'], index=['RelKey'], columns=['Season'], aggfunc=np.sum).fillna(0)

    # A little bit of house cleaning and rearrenging the columns sequence
    C_order=[]
    for y in R2T_Globals.YRS:
         for s in R2T_Globals.Seasons:
             C_order.append(s+str(y))
    gen_input=gen_input['MWseason'][C_order]

    return gen_input

def SaveData(OutFile, dframe):
    #	print_if('Writing output file {}'.format(OutFile))
    dframe.reset_index(drop=True, inplace=True)
    #	dframe['ID']=dframe.index.values
    print_if('Writing Power Plants table to file {}'.format(OutFile))
    dframe.to_csv(OutFile, index_label='Index')  # ,index=False)
    return


def SaveHydro(OutFile, dframe):
    dframe.reset_index(drop=True, inplace=True)
    print_if('Writing network to file {}'.format(OutFile))
    dframe.to_excel(OutFile, index=False)
    return

def SaveCapDelta(OutFile, dframe):
    dframe.reset_index(drop=True, inplace=True)
    print_if('Writing capacity deltas table to file {}'.format(OutFile))
    dframe.to_excel(OutFile, index=False)
    return

def SaveCapacity(OutFile, dframe):
    print_if('Writing ReEDS capacity table to file {}'.format(OutFile))
    dframe.to_excel(OutFile, index=False)
    return

def SaveGeneration(OutFile, dframe):
    dframe.reset_index(inplace=True)
    print_if('Writing ReEDS ganeration table to file {}'.format(OutFile))
    dframe.to_excel(OutFile, index=False)
    return


