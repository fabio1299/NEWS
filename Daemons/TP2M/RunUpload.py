#! /usr/bin/env python3
#

"""
Service RunUpload.py
Monitors folders for model results to upload to Link server
optionally calculates average of model results over GCMs
"""

# TODO Update to handle priming files

import glob,os,subprocess,sys,time,argparse, socket
from R2T_Globals import *
import R2T_Globals
from argparse import RawTextHelpFormatter
import paramiko
import requests, urllib
from scp import SCPClient
#from urllib import quote_plus as urlquote
from datetime import datetime
from pytz import timezone
import base64

#######################################################################

# List of files that will generated when the -a (average) option flag is set
AAC_files=['min','mean']


def date_str(tzn):
    # Adjusts time stamp based on time zone
    return datetime.now(timezone('UTC')).astimezone(timezone(tzn)).strftime('%Y-%m-%d %H:%M %Z')

def verStr(val):
    # resturs string format for version number
    return 'v{0:03d}'.format(val)

def AvrgServerPath(row):
    # returns web server path (from web server root) for
    # average result files (we don't need the RCP part)
    t = NamingLUT_Dict('tp2m', 'reeds')
    path='RCP' + t[row['rcp']] + "avg." + t[row['scenario']]
    return path

def NrmlServerPath(row):
    # returns web server path (from web server root) for
    # normal (non average) result files (we restore the GCM)
    t = NamingLUT_Dict('tp2m', 'reeds')
    path=AvrgServerPath(row).replace('RCP',t[row['gcm']] + '_').replace('avg.','_')
    return path


def AvrgPath(row):
    # returns local path for average result files
    # average resutl files are saved in the 'GCM_Means'
    # folder under the project folder
    path=MonitorFolder + '/GCM_Means/' + \
         row['rcp'] + '/' + \
         row['scenario'] + \
         '/' + verStr(row['version']) + \
         '/TP2M2ReEDS/'
    return path

def NrmlPath(row):
    # returns local path for normal result files
    # normal result files are saved in the <GCM> named
    # folder under the project folder
    path=AvrgPath(row).replace('GCM_Means',row['gcm'])
    return path

def AvrgName(row):
    # returns the root file name for average result files
    # on the local side
    # later we use the AAC_files list defined above to
    # get the actual file names for upload
    if row['type']=='AAC':
        name='AAC_' + row['rcp'] + '_' + \
             row['scenario'] + \
             '_'+ verStr(row['version']) # + '.csv'
    elif row['type']=='prime':
        name = 'temp' # TODO Need to define formats for average prime files
    else:
        print('Error average type, got {}'.format(row['type']))
        sys.exit(1)
    return name

def NrmlName(row):
    # returns the root file name for normal result files
    # on the local side
    # later we use the AAC_files list defined above to
    # get the actual file names for upload
    name=AvrgName(row).replace('AAC_','AAC_' + row['gcm'])
    # TODO Need to define formats for prime files
    return name

def FileExists(row):
    # Check for existance of result files
    exists = 0
    for type in AAC_files:
        exists += os.path.isfile(row['path'] + row['file'] + '_' + type + '.csv')
    return exists

def GetAverages(RefList,**kwargs):
    # Adds to the working dataframe the parameters needed for averaging and upload
    # used when the -a (average) flag is invoked
    print_if('Generating parameters for averaging')
    AvrgList=RefList.drop_duplicates(subset=['rcp', 'scenario','version','type']).copy()
    AvrgList['path']=AvrgList.apply(AvrgPath,axis=1)
    AvrgList['file']=AvrgList.apply(AvrgName,axis=1)
    AvrgList['exists'] = AvrgList.apply(FileExists, axis=1)
    return AvrgList

def GetNormal(RefList,**kwargs):
    # Adds to the working dataframe the parameters needed for upload
    # used for nnormal execution
    NrmlList=RefList[['gcm','rcp', 'scenario','version','type']].copy()
    NrmlList['path']=NrmlList.apply(NrmlPath,axis=1)
    NrmlList['file']=NrmlList.apply(NrmlName,axis=1)
    NrmlList['exists'] = True
    return NrmlList

def GetList(flist,**kwargs):
    # From files list in Scripts folder returns the list of
    # runs that are candidates for processing by the daemon
    ret=[]
    for f in flist:
        log=open(f.replace('Scripts','Logs').replace('.sh','.log'), 'r').read()
        if log.find('AAC files generation finished:') >= 0:
            # if the last line in the log file contains 'AAC files generation finished:'
            # we have AAC files ready for possible upload
            ret.append([f,'AAC'])
        if log.find('ReEDS priming finished:') >= 0:
            # if the last line in the log file contains 'ReEDS priming finished:'
            # we have priming files ready for possible upload
            ret.append([f, 'prime'])
    return pd.DataFrame({'script':[rec[0] for rec in ret],'type':[rec[1] for rec in ret]})

def GetParam(RefList, **kwargs):
    # adds naming and upload parameters to the main dataframe
    # parsing the script name
    t=NamingLUT_Dict('script','tp2m') #pd.Series(R2T_Globals.NamingConvention['tp2m'].values, index=R2T_Globals.NamingConvention['script'].astype(str)).to_dict()
    RefList['gcm']=RefList['script'].apply(lambda x : t[x.replace('.sh','').split('_')[2][0:3]])
    RefList['rcp']=RefList['script'].apply(lambda x : t[x.replace('.sh','').split('_')[2][3:]])
    RefList['scenario']=RefList['script'].apply(lambda x : t[x.replace('.sh','').split('_')[3]])
    RefList['version'] = RefList['script'].apply(lambda x : int(x.replace('.sh','').split('_')[4][1:]))
    return RefList

def CalcAvrg(df_list,gcms):
    # Handles the calsulation of the averages and minimum
    # other summary statistics required would need to be implemented
    # in this function
    if len(df_list.loc[df_list['exists'] == False]) == 0:
        print_if('No summary statistics calculations needed')
    else:
        print_if('Calculating required summary statistics')
    for i,rec in df_list.loc[df_list['exists'] == False].iterrows():
        basepath=rec['path']+rec['file']
        # The 'files' dictionary uses the file path as key and the
        # statistics type as value
        files={}
        NotFoundFlag = False
        for gcm in gcms:
            for type in AAC_files:
                file=basepath.replace('GCM_Means',gcm).replace('AAC_','AAC_' + gcm + '_') + '_' + type + '.csv'
                # for each GCM we need to check that the corresponding result file exists
                # otherwise we set the NotFoundFlag to abort the calculation of the statistics
                if os.path.isfile(file):
                    files[file]=type
                else:
                    print('AAC file not found for GCM {}, RCP {}, Scenario {}, Version {}'.format(gcm,rec['rcp'],rec['scenario'],verStr(rec['version'])))
                    NotFoundFlag=True
        if NotFoundFlag is False:
            # the AAC dictionary uses the statistics type as index and the summary dataframe
            # as value
            AAC={}
            # we first initialize the dictionary with None values
            for type in AAC_files:
                AAC[type]=None
            for file,type in files.items():
                # when the dictionary value is None we are loading the first dataset
                if AAC[type] is None:
                    AAC[type]=pd.read_csv(file)
                    tmp = None
                else:
                # else we laod to a temporary dataframe
                    tmp=pd.read_csv(file)
                if tmp is not None:
                # If we have data in the tmp dataframe we calcualte the statistics
                    if type == 'mean':
                        # for 'mean' we add the values for each GCM
                        AAC[type]['AAC']=AAC[type]['AAC'] + tmp['AAC']
                    else:
                        # for 'min' we add copy the new values in a tmp colum
                        # and then get the min between the existing and the new
                        AAC[type]['tmp']=tmp['AAC']
                        AAC[type]['AAC']=AAC[type][['AAC','tmp']].min(axis=1)
            # now for the 'mean' df, we divide the sum by the number of GCMs
            AAC['mean']['AAC']=AAC['mean']['AAC']/len(gcms)
            make_sure_path_exists(rec['path'])
            for type in AAC_files:
                AAC[type].to_csv(rec['path']+rec['file']+'_'+type+'.csv',columns=['AAC','Year','Season','BA','TECH','Cooling'])
            df_list.loc[df_list.index == i,['exists']]=len(AAC_files) #True
    return df_list

def upload(WrkList, average, **kwargs):
    # Add server path strucutre
    if average:
        WrkList['link_path'] = WrkList.apply(AvrgServerPath,axis=1)
    else:
        WrkList['link_path'] = WrkList.apply(NrmlServerPath, axis=1)

    user='newswebuser'
    password='UjQlRTVuWDclNW81ZDExeA=='

    dir_url='TP2M/'
    log_file='status.txt'
    remote_log = get_statmus(R2T_Globals.LinkURL + R2T_Globals.LinkServerURL + dir_url + log_file, user, password)
    # check current files on link server
    # compare to upload list
    upload_list = build_upload(remote_log, WrkList.copy())
    ### Upload list of directories to the remote server
    if len(upload_list) > 0:
        remote_log = put_data(upload_list, remote_log, user, password)
        set_status(remote_log, log_file, user, password)
    else:
        print_if("\tNothing to do: " + date_str(time_zone))
    return

def get_status(url, user, password):
    # Reads status file from web server
    log_list = []
    ssl_verify=True
    #my_proxies=urllib.getproxies()
    response = requests.get(url, auth=(user,base64.b64decode(password)), verify=ssl_verify) #, proxies=my_proxies)
    if response.status_code == 200:
        log_list = response.content.decode().rstrip("\n").split("\n")
        log_list = [rec.split("\t") for rec in log_list]

        print_if('Got response from "status.txt" of {0:d} lines with {1:d} elements'.format(len(log_list), len(log_list[0])))
    else:
        print('HTTPS connection to the remote server FAILED...\n')
        sys.exit()

    return log_list

def set_status(log, log_file, user, password):
    # writes status file to web server
    ssl_verify=True
    dir_url = 'TP2M/'
    cgi_url = R2T_Globals.LinkURL + R2T_Globals.LinkServerCGI + 'put_data.py'

    log_str = "\n".join("\t".join(rec) for rec in log) + "\n"
    r = requests.post(cgi_url, auth=(user,base64.b64decode(password)), files={'file': (log_file,log_str)}, params={'path': dir_url}, verify=ssl_verify) #, proxies=my_proxies)

    #if verbose:
    print_if("\tLog file updated:" + str(r.status_code == 200) + ':' + date_str(time_zone))

    return

#######################################################################

def put_data(dir_list, log, user, password):
    # writes data files to web server
    ssl_verify=True
    dir_url = 'TP2M/'
    cgi_url = R2T_Globals.LinkURL + R2T_Globals.LinkServerCGI + 'put_data.py'

    for index,rec in dir_list.iterrows():
        file_list={}
        success = True
        reftime=os.path.getmtime(os.path.join(rec['path'], rec['file'] + '_mean.csv'))
        #remotefilename=rec['file']
        for type in AAC_files:
            remotefilename='AAC_' + type + '.csv'
            localfilename=os.path.join(rec['path'], rec['file'] + '_' + type + '.csv')
            r = requests.post(cgi_url, auth=(user, base64.b64decode(password)),
                              files={'file': (remotefilename, open(localfilename, 'rb'))},
                              params={'path': os.path.join(dir_url, rec['link_path'])}, verify=ssl_verify) #
                              # , proxies=my_proxies)
            if r.status_code == 200:
                if rec['new'] and type == 'mean':
                    Host = socket.gethostname()
                    IP = socket.gethostbyname(Host)
                    #log.insert(0, [dataDir, date_str(time_zone), IP, Host])
                    log.insert(0, [rec['link_path'], date_str(time_zone), IP, Host, str(reftime),str(0)])
                print_if('Dir transfer status is "OK": ' + rec['link_path'])
            else:
                print_if('Dir transfer status is "Fail":' + rec['link_path'])
                break
    return log

def build_upload(remote_log,local_list):
    # Creates list of files to be uploaded
    local_list['upload']=False
    local_list['new']=False

    remote_list = [rec[0] for rec in remote_log]
    for index,run in local_list.iterrows():
        if run['link_path'] in remote_list:
            for rec in remote_log:
                if rec[0] == run['link_path'] and int(rec[5]):
                    remote_time = float(rec[4])
                    local_time = os.path.getmtime(os.path.join(run['path'], run['file'] + '_mean.csv'))
                    if (local_time - remote_time) > 1:
                        local_list.loc[local_list.index == index,'upload'] = True
                        rec[1] = date_str(time_zone)
                        Host = socket.gethostname()
                        rec[2] = socket.gethostbyname(Host)
                        rec[3] = Host
                        rec[4] = str(local_time)
                        rec[5] = str(0)
                    break
        else:
            local_list.loc[local_list.index == index,'new'] = True
            local_list.loc[local_list.index == index,'upload'] = True

    return local_list.loc[local_list['upload']==True].copy()


def main(**kwargs):
    # Main routine will run until a ^C is issued
    try:
        while (1):
            RefList=GetList(glob.glob(ScriptFolder + '/WBM_TP2M_*'))
            RefList=GetParam(RefList)
            if kwargs['average']:
                print_if('Working on GCM avarages')
                WrkList=GetAverages(RefList)
                WrkList=CalcAvrg(WrkList,list(RefList['gcm'].unique()))
            else:
                print_if('Generating working list')
                WrkList=GetNormal(RefList)
            # We only care to upload the most recent version (iteration) of the loop
            # So we find the current maximum
            MaxVer=WrkList.loc[WrkList['version'].idxmax()]['version']
            if not kwargs['no_upload']:
                # We only upload when:
                #   the file exists (the processing on the CCNY side is done)
                #   the version matches the most recent iteration
                upload(WrkList.loc[(WrkList['exists']==len(AAC_files)) & (WrkList['version']==MaxVer)].copy(), kwargs['average'])
            else:
                print_if('No upload requested... skipping.')
                break
            time.sleep(cycle_min * 60)        ### Sleep time: converts minutes to seconds
    except KeyboardInterrupt:
        print('\nKeyboardinterrupt: ...Program Stopped Manually!')
        sys.exit()

if __name__ == "__main__":

    """""
    main
    """""
    #############################################################################

    # Command line parameters parser

    parser = argparse.ArgumentParser(description='Allocates ReEDS output to a WBM network',
                                             formatter_class=RawTextHelpFormatter)

    #parser.add_argument('-b', '--base_folder',
    parser.add_argument(dest='MonitorFolder',
                        help="Base directory of TP2M project",
                        default="/asrc/ecr/NEWS/LoopingPapaer")

    parser.add_argument('-a', '--average',
                        dest='average',
                        action='store_true',
                        help="Average model results over GCMs\n" +
                             "\tDefault is: False",
                        default=True)

    parser.add_argument('-n', '--no_upload',
                        dest='no_upload',
                        action='store_true',
                        help="Does not upload files to link server, only calculates averages if requested",
                        default=False)

    parser.add_argument('-s', '--scripts',
                        dest='ScriptFolder',
                        help="Scripts folder within base directory (default=Scripts)",
                        default='Scripts')

    parser.add_argument('-z', '--time_zone',
                        dest='time_zone',
                        help="Time zone of local computer \n" +
                             "\tDefault value is: US/Eastern",
                        default='US/Eastern')

    parser.add_argument('-l', '--logs',
                        dest='LogFolder',
                        help="Logs folder within base directory (default=Logs)",
                        default='Logs')

    parser.add_argument('-v', '--verbose',
                        dest='print_if_flag',
                        action='store_true',
                        help="Prints some debugging info",
                        default=True)

    parser.add_argument('-t', '--time',
                        dest='cycle_min',
                        help="Wait time (in minutes) for next task after task completion (default=10)",
                        default=10)


    args = parser.parse_args()

    if args.print_if_flag:
        print(args)

    MonitorFolder = args.MonitorFolder # "/asrc/ecr/NEWS/LoopingPaper"
    ScriptFolder = os.path.join(MonitorFolder, args.ScriptFolder) # "/Scripts"
    LogFolder = os.path.join(MonitorFolder,args.LogFolder) #  "/Logs"

    cycle_min = int(args.cycle_min) # 10  ### Time interval in minutes between directory status re-checks
    time_zone = args.time_zone

    init()

    R2T_Globals.print_if_flag = args.print_if_flag #1

    main(average=args.average,no_upload=args.no_upload)



print("end")
