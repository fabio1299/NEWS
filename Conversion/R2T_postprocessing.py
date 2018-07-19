"""
R2T_postprocessing.py
"""
import R2T_Globals
from R2T_Globals import *
from R2T_readinputfiles import getCoordID,loadCoordID, \
                            getCoordStr,loadCombID
import subprocess as sp
import rgis as rg
import random
import csv

cool_sort={1:6,
           2:4,
           3:1,
           4:5,
           5:3,
           6:2
           }
cool_orig = {v: k for k, v in cool_sort.items()}

def copyGDBC(InFile,OutFile,OutTitle):
    Dir2Ghaas = R2T_Globals.Dir2Ghaas # '/usr/local/share/ghaas'
    cmd=[Dir2Ghaas + '/bin/setProperties','-t',OutTitle,InFile,OutFile]
    sp.call(cmd)

def setLayer(in_df):
    # This function sets the relevant layer for each power plant for each year

    p = in_df.set_index('CombID', drop=False)

    # COOLING CODE HAS CHANGED FROM TRADITIONAL USE SO THAT DRY AND RECIRC ARE CALCULATED FIRST IN TP2M.
    # THESE ARE CHANGED BACK TO TRADITIONAL CODES AT THE END.
    p['Cooling'].replace(cool_sort, inplace=True)

    #################################################
    # Now we calculate the layer of pertinance of each power plant
    # We first count the unique occurrences of each location
    UniqueLocations = pd.DataFrame(p.groupby('CoordID').CoordID.count().sort_values(ascending=False))
    UniqueLocations.columns.values[0] = 'Count'
    # we merge it with the input dataframe
    p = pd.merge(p, UniqueLocations, how='left', left_on='CoordID', right_index=True)
    # and sort on the count, location and cooling so that:
    #   the locations with the highest count are on top
    #   within each location the power plants with the lowest cooling are on top
    p.sort_values(['Count', 'CoordID', 'Cooling'], ascending=[False, True, True], inplace=True)
    # Now we find at which row we see a change in location (first time) and
    # repeat for (number of layers - 1) time using the newly created column as
    # shift (change) column. The result is something along the lines of:
    #                  Cooling  x_y       CoordID          NewCol1         NewCol2        NewCol3
    # CombID
    # 676.0_3.0_5.0        3.0    4   28.075_-81.925             NaN             NaN            NaN
    # 676.0_2.0_2.0        4.0    4   28.075_-81.925  28.075_-81.925             NaN            NaN
    # 676.0_5.0_2.0        4.0    4   28.075_-81.925  28.075_-81.925  28.075_-81.925            NaN
    # 676.0_5.0_1.0        6.0    4   28.075_-81.925  28.075_-81.925  28.075_-81.925  28.075_-81.925
    # 55299.0_3.0_5.0      3.0    4   29.725_-95.225             NaN             NaN            NaN
    # 90248.0_3.0_5.0      3.0    4   29.725_-95.225  29.725_-95.225             NaN            NaN
    # 10670.0_2.0_2.0      4.0    4   29.725_-95.225  29.725_-95.225  29.725_-95.225            NaN
    # 3461.0_5.0_2.0       4.0    4   29.725_-95.225  29.725_-95.225  29.725_-95.225  29.725_-95.225
    # ...                  ...  ...              ...             ...             ...             ...
    # 90001.0_5.0_2.0      4.0    1  46.825_-119.975             NaN             NaN             NaN

    for i in [1, 2, 3]:
        if i == 1:
            ShifItem = 'CoordID'
        else:
            ShifItem = 'NewCol' + str(i - 1)
        p['NewCol' + str(i)] = p.groupby('CoordID')[ShifItem].shift()
    # Replace all NaN with 0 (to simplify calculations)
    p.fillna(0, inplace=True)
    # Create a new column 'Layer' and set it to the # of layers we want (4 in our case)
    p['Layer'] = 4
    # and for every NewCol that is set to 0 we subtract 1 from 'Layer'
    for i in [1, 2, 3]:
        p.loc[p['NewCol' + str(i)] == 0, 'Layer'] -= 1

    # RESET THE COOLING BACK TO TRADITIONAL CODES.
    p['Cooling'].replace(cool_orig, inplace=True)

    return p[['CombID', 'CoordID', 'Layer','PlantCode','Fuel','Cooling','Longitude','Latitude']]


def LoadTemplate(rgGrid, used, LakeOcean):
    Rows = []
    for i in range(0, rgGrid.nRows):
        Lat=(rgGrid.URy - (rgGrid.ResY / 2 + rgGrid.ResY * i))
        Rows.append(getCoordStr(Lat))
#    Rows.sort(reverse=True)

    Cols = []
    for i in range(0, rgGrid.nCols):
        Lon=(rgGrid.LLx + rgGrid.ResX / 2 + rgGrid.ResX * i)
        Cols.append(getCoordStr(Lon))

    df_temp=pd.DataFrame(data=rgGrid.Data[0, :, :], index=Rows, columns=Cols)

    df_PP = setCells(df_temp.copy() , returnDictionary(used, 0.0))
    df_LakeOcean = setCells(df_PP.copy() , returnDictionary(LakeOcean, 1.0))
    #df_PPCodes = setCells(df_temp.copy() , returnDictionary(LakeOcean, 1))

    return df_PP,df_LakeOcean

def returnDictionary(ListKey,ListValue):
    if  type(ListValue) is not list:
        Const=ListValue
        ListValue = [Const] * len(ListKey)
    return dict(zip(ListKey, ListValue))

def setCells(df,setDictionary):
    for key, value in setDictionary.items():
        lon,lat=key.split('_')
        df.loc[lat,lon]=value
    return df

def saveGDBC(rgGrid,df,GDBCfile,DatasetTitle,year=0, layer=0):
    if df is not None:
        if rgGrid.nLayers > 1:
            for i in range(0,rgGrid.nLayers):
                rgGrid.Data[i, :, :] = df[i].values
        else:
            rgGrid.Data[0,:,:]=df.values
    if year != 0:
        title = DatasetTitle + ", {}, Layer {}".format(year,layer)
    else:
        title = DatasetTitle
    if rgGrid.nLayers == 12:
        rgGrid.SaveAs(GDBCfile, R2T_Globals.GDBCtemplate, title, True, 'month', year, 1)
    elif rgGrid.nLayers == 1:
        rgGrid.SaveAs(GDBCfile,R2T_Globals.GDBCtemplate,title)
    else:
        raise Exception('Wrong number of layers ({}) in output GDBC file {}'.format(rgGrid.nLayers,GDBCfile))

    # Now we copy the dataset to the missing year
    # For the years 2010 and 2050 we need to expand in the past and future
    # e.g. 2010 = 2000,2001,2003,...,2009
    #      2050 = 2051,2052,2053,...,2059
    # For all other years current year = current year + 1
    if 2010 < year < R2T_Globals.YEAR_E:
        NewFile=GDBCfile.replace(str(year),str(year+1))
        NewTitle=title.replace(str(year),str(year+1))
#        DatasetTitle=DatasetTitle.replace(str(year),str(year+1))
#        GDBCfile=
        if rgGrid.nLayers == 12:
            temp = "/tmp/tmp" + str(random.randint(10000, 99999)) + ".gdbc"
            copyGDBC(GDBCfile, temp, NewTitle)
            rgGrid.DateLayer(temp,NewFile,'month',year+1,1)
            os.remove(temp)
        else:
            copyGDBC(GDBCfile, NewFile, NewTitle)
            #cmd = 'cp ' + GDBCfile + ' ' + NewFile
            #sp.call(cmd,shell=True)
#            Layers['Name'] = rgGrid.Layers['Name'].apply(lambda x: x.replace(str(year),str(year+1)))
#        rgGrid.SaveAs(GDBCfile, R2T_Globals.GDBCtemplate, DatasetTitle,True,'month',y,)
    else:
        if year == 2010:
            years=list(range(2000,2012))
            years.reverse()
        elif year == 2050:
            years = list(range(2051, 2060))
        elif year == 0:
            return
        else:
            print('Unrecognized year value ({}) in year expansion loop'.format(year))
#        for i in range(0,len(years)):
#            if i == 0:
#                fyear=str(year)
#                tyear=str(years[i])
#            else:
#                fyear=str(years[i - 1])
#                tyear=str(years[i])
#            DatasetTitle = DatasetTitle.replace(fyear,tyear )
        for tyear in years:
            if tyear != year:
                NewFile = GDBCfile.replace(str(year), str(tyear))
                NewTitle = title.replace(str(year), str(tyear))
                if rgGrid.nLayers == 12:
                    temp = "/tmp/tmp" + str(random.randint(10000, 99999)) + ".gdbc"
                    copyGDBC(GDBCfile, temp, NewTitle)
                    rgGrid.DateLayer(temp, NewFile, 'month', tyear, 1)
                    os.remove(temp)
                    #rgGrid.DateLayer(GDBCfile, NewFile, 'month', tyear, 1)
                else:
                    copyGDBC(GDBCfile, NewFile, NewTitle)
                    #cmd = 'cp ' + GDBCfile + ' ' + NewFile
                    #sp.call(cmd, shell=True)

def saveGDBP(data,year,layer,outpath, BaseTitle=""):
    if data.empty:
        return
    title="Power Plant Locations{}, {}, Layer {}".format(BaseTitle,year,layer)
    Dir2Ghaas = R2T_Globals.Dir2Ghaas # '/usr/local/share/ghaas'
    temp="/tmp/tmp"+str(random.randint(10000,99999))
    OutFile=outpath + "PP_" + str(layer) + "_" + str(year) + '.gdbp'
    print_if('Saving power plant configuration for year {} layer {} to file:'.format(year,layer))
    print_if('\t{}'.format(OutFile))
    data[['PlantCode','Fuel','Cooling','Longitude','Latitude']].to_csv(temp + '.csv',index=False, sep='\t',quoting=csv.QUOTE_NONNUMERIC)
    cmd=[Dir2Ghaas + '/bin/table2rgis']
    cmd.append(temp + '.csv')
    cmd.append(temp + '.gdbt')
    sp.call(cmd)
    cmd=[Dir2Ghaas + '/bin/tblConv2Point','-a','DBItems','-x','Longitude','-y','Latitude','-t',title]
    cmd.append(temp + '.gdbt')
    cmd.append(OutFile)
    sp.call(cmd)
    os.remove(temp+'.csv')
    os.remove(temp+'.gdbt')

def genGDBC(capacity, efficiency, generation, template, start_year, end_year, outpath,compress=False):
    # We set the extension of the output files based on the value of the compress flag

    if compress:
        FileExt='.gdbc.gz'
    else:
        FileExt = '.gdbc'

    # Get the GCM, RPC, Scenario and Version from the outpath
    #PathComp=outpath.split('/')
    #Version=int(PathComp[-4][1:])
    #Scenario=PathComp[-5]
    #RCP=PathComp[-6]
    #GCM=PathComp[-7]

    GCM,RCP,Scenario,Version=getSimulationFromPath(outpath)

    BaseTitle=': {}, {}, {}, Ver: {} '.format(Scenario,GCM,RCP,Version)

    # We load the GDBC that will serve as template for the yearly datasets
    rgYearTemplate = rg.grid(template)
    rgYearTemplate.Load()

    # And create the one that will server as template for the monthly datasets
    rgMonthTemplate = rgYearTemplate.Copy()

    rgMonthTemplate.Layers.loc[0,'Name']='XXXX-01'

    # By adding 11 more layers
    """
    for i in range(2,13):
        name='XXXX-' + '{num:02d}'.format(num=i)
        rgMonthTemplate.AddLayer(name)
    """
    NewNames = ['XXXX-' + '{num:02d}'.format(num=i) for i in range(2, 13)]
    rgMonthTemplate.AddLayer(NewNames, 11)

    # And generate a template dataframe that will be used to load all the info
    # and we also get the LakeOcean dataframe
    uniqueCells = list(capacity.CoordID.unique())
    LakeOcean = list(capacity.loc[(capacity['LakeOcean'] == 1) | (capacity['AltWater'] == 1)].CoordID.unique())
    #PPtemplate,df_LakeOcean,df_PPCodes = LoadTemplate(rgYearTemplate, uniqueCells, LakeOcean)
    PPtemplate,df_LakeOcean = LoadTemplate(rgYearTemplate, uniqueCells, LakeOcean)

    # And save the LakeOcean gdbc
    saveGDBC(rgYearTemplate.Copy(), df_LakeOcean, outpath + "LakeOcean" + FileExt, "LakeOcean" + BaseTitle)

    # We initialize a list that will hold the names of the empty datasets (e.g. the year-Layers
    # combinations that have all capacity at 0
    # We'll use this list to generate these files off the loop
    for y in range(start_year,end_year + 1, 2):
        # We first extract the cells that are used in the curret year
        print_if(y)
        # The following statement makes sure that only the
        #df_layers = setLayer(capacity.loc[capacity['NP_' + str(y)] > 0])
        df_layers = setLayer(capacity.loc[capacity['NP_' + str(y)] >= 1.0])
        # We iterate on the layers
        for layer in range(1,5):
            PPinLayer=df_layers.loc[df_layers['Layer'] == layer,['CombID','Latitude','Longitude','PlantCode','Fuel','Cooling']]
            # From the PPinLayer dataframe we create the GDBP for post analysis
            # eventually this step can be avoided and we can simpy use a numpy mask
            saveGDBP(PPinLayer, y, layer, outpath, BaseTitle)
            # We first save the yearly datasets
            suffix = '_' + str(layer) + '_' + str(y)
            col = 'NP_' + str(y)
            for dataset in ['NP','Fuel','Cooling']:
                OutFile=outpath + dataset + suffix + FileExt
                if dataset != 'NP':
                    col = dataset
                Title = dataset + BaseTitle # + str(y)
                if PPinLayer.empty:
                    saveGDBC(rgYearTemplate.Copy(), PPtemplate, OutFile, Title, y, layer)
                else:
                    capsel = capacity.loc[capacity['CombID'].isin(PPinLayer['CombID']), ['CoordID', col]]
                    tmp = PPtemplate.copy()
                    saveGDBC(rgYearTemplate.Copy(), setCells(tmp,capsel.set_index('CoordID')[col].to_dict()), OutFile, Title, y, layer)
                    del tmp
            col = 'Eff_' + str(y)
            OutFile = outpath + 'Eff' + suffix + FileExt
            Title='Efficiency' + BaseTitle # + str(y)
            if PPinLayer.empty:
                saveGDBC(rgYearTemplate.Copy(), PPtemplate, OutFile, Title, y, layer)
            else:
                effsel = efficiency.loc[efficiency['CombID'].isin(PPinLayer['CombID']), ['CoordID', col]]
                tmp = PPtemplate.copy()
                saveGDBC(rgYearTemplate.Copy(), setCells(tmp, effsel.set_index('CoordID')[col].to_dict()), OutFile, Title, y, layer)
                del tmp
            # And then the monthly
            tmpMonthly=rgMonthTemplate.Copy()
            # And now we update the GDBC layers' names with the current year
            tmpMonthly.Layers['Name'] = tmpMonthly.Layers['Name'].apply(lambda x: x.replace('XXXX', str(y)))
            cols=['CoordID']
            for m in R2T_Globals.Months:
                cols.append(m + str(y))
            suffix = '_' + str(layer) + '_mTS' + str(y)
            OutFile = outpath + 'Gen' + suffix + FileExt
            Title='Generation' + BaseTitle # + str(y)
            if PPinLayer.empty:
                OutData = {}
                for m in range(0, 12):
                    OutData[m] = PPtemplate.copy() # setCells(tmp, gensel.set_index('CoordID')[cols[m + 1]].to_dict())
            else:
                gensel = generation.loc[generation['CombID'].isin(PPinLayer['CombID']), cols]
                OutData={}
                for m in range(0,12):
                    tmp=PPtemplate.copy()
                    OutData[m]=setCells(tmp, gensel.set_index('CoordID')[cols[m+1]].to_dict())
                    del tmp
            saveGDBC(tmpMonthly.Copy(),OutData,OutFile,Title, y, layer)
            del tmpMonthly
        # Now we copy the current year in the year + 1

        #uniqueCells = list(capacity.loc[capacity['NP_' + str(y)] > 0].CoordID.unique())
        #PPtemplate = LoadTemplate('/asrc/ecr/NEWS/PowerPlants/PPtemplate.gdbc', uniqueCells)

if __name__ == "__main__":

    R2T_Globals.init()
    BasePath='/asrc/ecr/NEWS/configurations/TP2MInput/Water_output_HadGEM2_ES_RCP8p5_CAP/'
    OutputPath=BasePath+'PPgdbc/'
    capacity = pd.read_csv(BasePath+'capoutput.csv')
    efficiency = pd.read_csv(BasePath+'effoutput.csv')
    generation = pd.read_csv(BasePath+'genoutput.csv')

    # We add the CoordID to all the files
    capacity['CoordID']=capacity.apply(loadCoordID,axis=1)
    efficiency['CoordID']=efficiency.apply(loadCoordID,axis=1)
    generation['CoordID']=generation.apply(loadCoordID,axis=1)

    # And the CombID
    capacity['CombID'] = capacity.apply(loadCombID,axis=1)
    efficiency['CombID'] = efficiency.apply(loadCombID, axis=1)
    generation['CombID'] = generation.apply(loadCombID, axis=1)

    # Now we can process the files and load the data in the GDBC files
    genGDBC(capacity, efficiency, generation, R2T_Globals.GDBCtemplate, 2010, 2050, OutputPath, True)

