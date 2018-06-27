
"""
R2T_Globals.py
"""

import pandas as pd
import os
import sys
import errno
from ast import literal_eval


def init():
    """""
    vars
    """""
    global PCAs,YEAR_S, YEAR_E, YRS, DBYears, OutputDir, Dir2Ghaas
    global Resolution, capacity_factor, GDBCtemplate, SaveDir
    global YEAR_DB_S, YEAR_DB_E, YEAR_AN_S, YEAR_AN_E, YEAR_RE_S, YEAR_RE_E
    global LinkServer,LinkServerReEDS,LinkServerTP2M,LinkServerURL,LinkServerCGI
    global LinkURL,LinkServerLocalCGI
    PCAs = [x for x in range(1, 135)] # NE PCAs
    YEAR_DB_S = 2010
    YEAR_DB_E = 2015
    YEAR_AN_S = 2000
    YEAR_AN_E = 2059
    YEAR_RE_S = 2010
    YEAR_RE_E = 2050
    YEAR_S, YEAR_E = 0, 2050
    YRS = [] # x for x in range(YEAR_S, YEAR_E + 1, 2)]
    DBYears=[]
    OutputDir='./'
    SaveDir='./'
    Resolution=0.05
    capacity_factor=1
    GDBCtemplate='/asrc/ecr/NEWS/PowerPlants/PPtemplate.gdbc'
    LinkServer='sneezy.ccny.cuny.edu'
    # Remote server root URL
    LinkURL = 'https://news-link.environmentalcrossroads.org/'
    LinkServerReEDS='/var/www/news_link/news_transfer/ReEDS'
    LinkServerTP2M='/var/www/news_link/news_transfer/TP2M'
    LinkServerURL='news_transfer/'
    LinkServerCGI='cgi-bin/'
    LinkServerLocalCGI='cgi-bin/news_link/'
    TimeZone=''

    if 'GHAASDIR' in os.environ:
        Dir2Ghaas = os.environ['GHAASDIR']
    else:
        Dir2Ghaas = '/usr/local/share/ghaas'

    """""
    flags
    """""
    global SavePP, SaveCells, SaveDeltas, SaveCap, SaveGen
    global merge6_8and3_8, print_if_flag
    merge6_8and3_8=True
    SavePP = False
    SaveCells = False
    SaveDeltas = False
    SaveCap = False
    SaveGen = False
    print_if_flag=False
    #ProjectBase = '/asrc/ecr/NEWS/MultiScenario/'
    #ProjectBase='/asrc/ecr/NEWS/tmp/'

    """
    more global vars
    """

    global ProjectHome,ProjectBase,ScriptBase,LogFile

    # ProjectHome is the main folder were all the Environment folder is located
    ProjectHome = '/asrc/ecr/NEWS/LoopingPaper/'
    # ProjectBase is the output folder were processing happens, by default it is the same
    # as ProjectHome, but it can be redirected to allow for different runs and/or debugging
    # redirection happens from the MainLink.py program passing the ProjectBase path with the
    # -o option
    ProjectBase = '/asrc/ecr/NEWS/LoopingPaper/'
    ScriptBase='WBM_TP2M_'
    LogFile=''



    """""
    dicts & lookups loaded from ReEDS_TP2M_LookupsActive.xlsx file
    """""
    global fuels, cooling, tech_specs, PCACentroids,cool_eff_lookup
    global time_lkup, Seasons, Fuels, Months, season_to_month, NamingConvention
    global ReEDS_files, ReEDS_files_GDX

    # We load the data from the excel file and then we parse each worksheet
    lookupData=read_excel('./ReEDS_TP2M_LookupsActive.xlsx')

    # Here we load the data for the fuel types. This lookup table needs to be converted
    # to a dictionary for backwards compatibility
    df_fuels=lookupData.parse('fuels')

    fuels={}

    for index, row in df_fuels.iterrows():
        fuels[row['ReEDS']]=str(row['TP2M'])


    # Here we load the data for the cooling technology. This lookup table needs to be converted
    # to a dictionary for backwards compatibility
    df_cooling=lookupData.parse('cooling')

    cooling={}

    for index, row in df_cooling.iterrows():
        cooling[row['ReEDS']]=str(row['TP2M'])

    # And now we load the Tech_specs lookup table. This lookup needs to be converted to a
    # dictionary with keys defined by the tuple of (fuel, cooling). The structure of the
    # dictionary is defined as:
    # (Fuel, Cooling)
    #   -> [Max Plant Size,
    #       Min Generator to be Added,
    #       gallons/MWh,
    #       m3/s (rate of withdrawal per MW for production (based on plant type)),
    #       new efficiencies]

    df_tech_specs=lookupData.parse('tech_specs')

    tech_specs={}

    for index, row in df_tech_specs.iterrows():
        tech_specs[literal_eval(row.iloc[0])]=[row.iloc[1] if row.iloc[1] > 0 else float('inf'),
                               row.iloc[2],row.iloc[3],row.iloc[4],
                               row.iloc[5] if row.iloc[5] > 0 else -float('inf')]


    # Now we load the lookup of the time slices and corresponding hours and season
    # this lookup table is used directly in its Dataframe form
    time_lkup=lookupData.parse('time_lkup')

    # We load the season to months lookup table. Also this lookup need to be
    # converted to a dictionary
    df_season_to_month=lookupData.parse('season_to_month')

    season_to_month=df_season_to_month.to_dict(orient='list')

    for s in season_to_month:
        season_to_month[s]=[x for x in season_to_month[s] if str(x) != 'nan']


    # And we load the PCA centroid coordinates for cooling types 8
    PCACentroids = lookupData.parse('PCACentroids')

    # And we load the cooling-efficiency lookup table
    cool_eff_lookup = lookupData.parse('Efficiency')

    # And we load the model naming convention table

    NamingConvention = lookupData.parse('ModelsCodes')

    # Now we extract the fuel types to be used in the TP2M analysis
    # these are the fuel types included in the tech_specs lookup table
    Fuels=[]
    for i in tech_specs.keys():
        if i[0] not in Fuels:
            Fuels.append(i[0])

    # And then we initialize some lists used later in the program
    Seasons = ['winter','spring','summer','fall']

    Months = ['Jan', 'Feb','Mar', 'Apr', 'May','Jun', 'Jul', 'Aug','Sep', 'Oct','Nov', 'Dec']

    ReEDS_files = ['consump.csv', 'convqctmnallm.csv', 'convqctmn.csv', 'convqctn.csv',
                   'ctupgrades.csv', 'wataccess.csv', 'watqty.csv', 'withdrawal.csv']

    ReEDS_files_GDX = ['reporting.gdx', 'water_output.gdx']


"""""
helpers
"""""
def to_id(*args):
    """
    code -> ID
    """
    return int(''.join([str(a) for a in args]))


def log(name,*args):
    """
    write leftover error to file
    """
    with open(name+'_err.txt', 'a') as out: out.write(str(args)+'\n')


def get_key(t):
    """
    extract key (PCA, fuel, cool); change cool if necessary
    note: (gas, {once,recirc,dry}) : (3,x) -> (3,x+3)
    """

#    k = int(t[7][1:]), int(fuels[t[5]]), int(cooling[t[6]])
    k = int(t[7][1:]), int(fuels[t[5]]), int(cooling[t[6]])
    if k[1] == 3 and k[2] in (1, 2, 3):  k = (k[0], 3, k[2] + 3)
    return k

# The following is a small routine that can help with debugging

def print_if(value,ending='\n'): #,flushing=True):
    if print_if_flag:
        if LogFile == '':
            Log=None
        else:
            Log=open(LogFile, 'a')
        print(value,end=ending,flush=True,file=Log) #,flush=flushing)
        if Log != None:
            Log.close()

def make_sure_path_exists(path):
    try:
        os.makedirs(path,0o2775)
        dirs=path.split('/')
        base='/'+dirs[1]+'/'
        for dir in dirs[2:]:
            base=base+dir+'/'
            if os.stat(base).st_uid == os.getuid():
                os.chmod(base, 0o2775)
#        for root, dirs, files in os.walk(path):
#            for d in dirs:
#                curPath=os.path.join(root, d)
#                if os.stat(curPath).st_uid == os.getuid():
#                    os.chmod(curPath, 0o2775)
#            for f in files:
#                curPath=os.path.join(root, f)
#                if os.stat(curPath).st_uid == os.getuid():
#                    os.chmod(curPath, 0o2775)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def SetRelKey(row):
    return str(int(row['PCA'])) + '_' + str(int(row['Fuel'])) + '_' + str(int(row['Cooling']))

def read_excel(infile):
    # This function buffers an excel file and allows leater extraction of individual
    # datasheets
    xlsx = pd.ExcelFile(infile)
    return xlsx

def getSimulationFromPath(pathName):
    # Get the GCM, RPC, Scenario and Version from the outpath
    PathComp = pathName.split('/')
    #    PathComp='/asrc/ecr/NEWS/LoopingPaper/hadgem2-es/rcp8p5/nc_bau/v000/ReEDS2TP2M/TP2Minput/'.split('/')
    Version = int(PathComp[-4][1:])
    Scenario = PathComp[-5]
    RCP = PathComp[-6]
    GCM = PathComp[-7]

    return GCM,RCP,Scenario,Version

def NamingLUT_Dict(f,t):
    if f not in NamingConvention.columns:
        raise ValueError("Function NamingLUT_Dict error: {} not present in NamingConvention LUT".format(f))
    if t not in NamingConvention.columns:
        raise ValueError("Function NamingLUT_Dict error: {} not present in NamingConvention LUT".format(t))
    if f != t:
        d = pd.Series(NamingConvention[t].values,
                  index=NamingConvention[f].astype(str)).to_dict()
    else:
        print('Function NamingLUT_Dict warning: from and to item is the same. Nothing to do')
        return {}
    return d


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
            raise Exception('Path does not exist')

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def CurTime():
    ts = time.time()
    return pd.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

