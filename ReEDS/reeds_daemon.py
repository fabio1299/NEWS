#!/usr/bin/python
# -*- coding: utf-8 -*-

########################################################################################################
#
#   reeds_daemon.py
#        Model output transfer using secure connection
#
#   Written by Dr. A. Prusevich (alex.proussevitch@unh.edu) and Stanley Glidden (stanley.glidden@unh.edu)
#   Updated - March, 2017
#
#   Modified to support sub folders on October 2017 (Fabio)
#   February 2018 added by Fabio:
#       moved configuration to externale file
#       added command-line parameter parser, for help use:
#           python reeds_daemon.py -h
#       added saving of timestamp and status flag on the server log
#       TP2M results download from server
########################################################################################################

from __future__ import print_function
#import news_transfer_config as cfg
import base64
import os, sys, socket, re
import time, getpass
import requests, urllib
import shutil
import errno
from datetime import datetime
from pytz import timezone
import importlib
import argparse
from argparse import RawTextHelpFormatter


#################################################################################################################
my_proxies      = urllib.getproxies()

######################  Main Code  ####################################

def main(): #argv):

    ### Turn off buffered output
    #if verbose:
    #    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    ### Check if data search directories exist
    error=False
    if not os.path.isdir(data_dir_upload):
        print('Upload data search directory does not exist:')
        print('\t',data_dir_upload)
        error=True
    if not os.path.isdir(data_dir_down):
        print('Download data directory does not exist:')
        print('\t', data_dir_down)
        error = True
    if error:
        sys.exit()

#######################################################################
##############   Data Directory Check and File Transfer   #############

    try:
        while (1):
            ## We first check if we need to upload new results
            ### Build list of directories to download
            # We get the log file from the server
            remote_log        = get_status( dir_up, user, password )

            # The local list is all the folders inside the ReEDS run folder that do not have
            # a VER number (the ones with a VER number are the older runs)
            # We also check for the existence of the file AAC.csv in the folder
            # (this indicates that the ReEDS run is done)
            local_list        = [rec for rec in os.listdir(data_dir_upload)
                                 if os.path.isdir(os.path.join(data_dir_upload,rec))
                                 and os.path.isfile(os.path.join(data_dir_upload,rec,'aac.csv'))
                                 and bool(re.match(r'^(?!_)', rec))
                                 and not bool(re.search('.VER', rec))]  ### Exclude also directories that start with "_"
            upload_list = build_upload(remote_log,local_list)

            ### Upload list of directories to the remote server
            if upload_list:
                remote_log = put_data( upload_list, remote_log, user, password )
                set_status(remote_log, log_file, dir_up, user, password)
            else:
                print_if("\tNothing to upload: " + date_str(time_zone))

            ## Now we check if we have files to download from the link server
            # we get the server log file
            remote_log = get_status(dir_down, user, password)
            # and the local folder
            local_list = [rec for rec in os.listdir(data_dir_down)
                          if os.path.isdir(os.path.join(data_dir_down, rec))
                          and bool(re.match(r'^(?!_)', rec))
                          and not bool(re.search('.VER', rec))]
            down_list = build_down(remote_log, local_list)
            ### Download list of directories from the remote server
            if down_list:
                remote_log = get_data( down_list, remote_log, user, password )
                set_status(remote_log, log_file, dir_down, user, password)
                ### TODO Call ReEDS and run model
            else:
                print_if("\tNothing to download: " + date_str(time_zone))

            time.sleep(cycle_min * 60)        ### Sleep time: converts minutes to seconds
    except KeyboardInterrupt:
        print('\nKeyboardinterrupt: ...Program Stopped Manually!')
        sys.exit()

#######################################################################
######################  Functions  ####################################

def get_status(dir, user, password):

    url=glob_url + dir + log_file
    log_list = []
    response = requests.get(url, auth=(user,base64.b64decode(password)), verify=ssl_verify, proxies=my_proxies)
    if response.status_code == 200:
        log_list = response.content.rstrip("\n").split("\n")
        log_list = [rec.split("\t") for rec in log_list]

        print_if('Got response from "status.txt" of {0:d} lines with {1:d} elements'.format(len(log_list), len(log_list[0])))
    else:
        print('HTTPS connection to the remote server FAILED...\n')
        sys.exit()

    return log_list

#######################################################################

def set_status(log, log_file, dir, user, password):

    log_str = "\n".join("\t".join(rec) for rec in log) + "\n"
    r = requests.post(cgi_url, auth=(user,base64.b64decode(password)), files={'file': (log_file,log_str)}, params={'path': dir}, verify=ssl_verify, proxies=my_proxies)

    print_if("\tLog file updated:" + str(r.status_code == 200) + ':' + date_str(time_zone))

#######################################################################

def put_data(dir_list, log, user, password):

    for dataDir,New in dir_list.iteritems():
        file_list={}
        success = True
        nFiles = 0
        data_Dir = os.path.join(data_dir_upload,dataDir,sub_dir)

        for rec in os.listdir(data_Dir):
            if os.path.isfile(os.path.join(data_Dir, rec)):
                mtime=os.path.getmtime(os.path.join(data_Dir, rec))
                file_list[rec]=mtime

        for fname,ftime in file_list.iteritems():
            if fname not in fileList:
                continue
            if fname == reffile:
                reftime=ftime
            r = requests.post(cgi_url, auth=(user,base64.b64decode(password)), files={'file': (fname,open(os.path.join(data_Dir,fname),'rb'))}, params={'path': os.path.join(dir_up,dataDir,sub_dir)}, verify=ssl_verify, proxies=my_proxies)
            if r.status_code != 200:
                success        = False
            else:
                nFiles += 1;

        ### Update status log list
        if success and nFiles:
            if New:
                Host = socket.gethostname()
                IP = socket.gethostbyname(Host)
                log.insert(0, [dataDir, date_str(time_zone), IP, Host, str(reftime),str(0)])
            rename_step(dataDir)
            print_if('Dir upload status is "OK": ' + dataDir)
        else:
            if nFiles:
                print_if('Dir upload status is "Fail":' + dataDir)
            else:
                print_if('Dir upload status is "Empty":' + dataDir)

    return log

#######################################################################

def get_data(download_list,remote_log,user, password):
    for data,timestamp in download_list.iteritems():
        path=os.path.join(data_dir_down,data)
        make_sure_path_exists(path)
        for file in downList:
            if file == 'aac_mean.csv':
                locfile='aac.csv'
            elif file == 'aac_min.csv':
                locfile = 'aacres.csv'
            else:
                print_if('Unknown file type to download')
                sys.exit(1)
            local_filename=os.path.join(path,locfile)
            url = glob_url + dir_down + data + '/' + file
            if get_file(url, local_filename, timestamp,user, password):
                print_if('Dir download status is "OK": ' + data)
                for rec in remote_log:
                    if rec[0]==data:
                        rec[5]=str(1)
                        break
            else:
                print_if('Dir download status is "Fail": ' + data)
    return remote_log

#######################################################################

def get_file(url,local_filename,servertimestamp,user, password):
    with requests.get(url, stream=True, auth=(user,base64.b64decode(password)),verify=ssl_verify, proxies=my_proxies) as r:
        try:
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            os.utime(local_filename,(servertimestamp,servertimestamp))
            success=True
        except:
            success = False
    return success

#######################################################################

def make_sure_path_exists(path):
    try:
        os.makedirs(path,0o2775)
        if os.name == 'posix':
            dirs=path.split('/')
            base='/'+dirs[1]+'/'
            for dir in dirs[2:]:
                base=base+dir+'/'
                if os.stat(base).st_uid == os.getuid():
                    os.chmod(base, 0o2775)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

#######################################################################

def date_str(tzn):
    return datetime.now(timezone('UTC')).astimezone(timezone(tzn)).strftime('%Y-%m-%d %H:%M %Z')

#######################################################################

def print_if(value,ending='\n'):
    if verbose:
        if LogFile == '':
            Log=None
        else:
            Log=open(LogFile, 'a')
        print(value,end=ending,file=Log)
        if Log != None:
            Log.close()

#######################################################################

def rename_step(dataDir):
    os.rename(data_dir_upload+dataDir,data_dir_upload+dataDir+set_version(dataDir))

#######################################################################

def set_version(dataDir):
    loops = [int(rec[-3:]) for rec in os.listdir(data_dir_upload)
            if os.path.isdir(os.path.join(data_dir_upload, rec))
            and bool(re.match(dataDir + '.VER', rec))]
    if len(loops) > 0:
        next_ver='.VER' + '{0:03d}'.format(max(loops) + 1)
    else:
        next_ver = '.VER000'
    return next_ver

#######################################################################

def build_down(remote_log, local_list):

    download_list={}
    for run in remote_log:
        if int(run[5]):
            break
        if run[0] in local_list:
            for rec in local_list:
                if run[0] == rec:
                    remote_time = float(run[4])
                    data_Dir=os.path.join(data_dir_down,rec)
                    file=os.path.join(data_Dir, 'aac_mean.csv')
                    if os.path.isfile(file):
                        local_time = os.path.getmtime(file)
                        if (remote_time - local_time) > 1:
                            download_list[run[0]] = float(run[4]) # ]=False
                    else:
                        download_list[run[0]] = float(run[4])
                    break
        else:
            download_list[run[0]] = float(run[4])
    return  download_list

#######################################################################

def build_upload(remote_log,local_list):
    # We instantiate an empty dictionary.
    # The key will be the folder to be uploaded,
    # the value:
    #       True if the upload is new
    #       False if it is an update
    upload_list={}

    remote_list = [rec[0] for rec in remote_log]
    for run in local_list:
        if run in remote_list:
            for rec in remote_log:
                if rec[0] == run and int(rec[5]):
                    remote_time = float(rec[4])
                    data_Dir=os.path.join(data_dir_upload,run,sub_dir)
                    local_time = os.path.getmtime(os.path.join(data_Dir, reffile))
                    if (local_time - remote_time) > 1:
                        upload_list[run]=False
                        rec[1] = date_str(time_zone)
                        Host = socket.gethostname()
                        rec[2] = socket.gethostbyname(Host)
                        rec[3] = Host
                        rec[4] = str(local_time)
                        rec[5] = str(0)
                    break
        else:
            upload_list[run]=True
    return upload_list

#########################################################################

if __name__ == "__main__":
    # We first read the command line arguments and check that we have all the info needed
    # to run the routine

    parser = argparse.ArgumentParser(description='ReEDS side daemon for the WBM/TP2M-ReEDS link ',
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('configuration',
                        help="Python script with environment configuration (e.g. news_transfer_config.py)")

    parser.add_argument('-v', '--ssl_verify',
                        dest='ssl_verify',
                        help="SSL verification method- True/False or path to CD_BUNDLE *.pem file \n" +
                             "\tDefault is: True",
                        default=True)

    parser.add_argument('-z', '--time_zone',
                        dest='time_zone',
                        help="Time zone of local computer \n" +
                             "\tDefault value is: US/Mountain",
                        default='US/Mountain')

    parser.add_argument('-t', '--cycle_min',
                        dest='cycle_min',
                        help="Time interval in minutes between directory status re-checks \n" +
                             "\tDefault value is: 10",
                        default=10)

    parser.add_argument('-V', '--verbose',
                        dest='verbose',
                        action='store_true',
                        help="Prints debugging info",
                        default=False)

    parser.add_argument('-l', '--logfile',
                        dest='logfile',
                        help="Writes output to logfile, implies -V (verbose) flag",
                        default='')

    args = parser.parse_args()

    if os.path.isfile(args.configuration):
        path,module=os.path.split(args.configuration)
        module=module.replace('.py','')
        cfg = importlib.import_module(module,args.configuration )
    verbose = args.verbose
    cycle_min = float(args.cycle_min) # 10  ### Time interval in minutes between directory status re-checks
    time_zone = args.time_zone # 'US/Mountain'
    ssl_verify = args.ssl_verify # True  ### SSL verification method- True/False or path to CD_BUNDLE *.pem file
    LogFile=args.logfile
    if LogFile != '':
        verbose = True

    data_dir_upload = cfg.data_dir_upload #'X:/FY17-NEWS-SMC/Full 60 scenarios/201705_round3/scenarios_NoCPP/'  # Local path to the upload data
    data_dir_down = cfg.data_dir_down # Local path to the download data
    sub_dir = cfg.sub_dir #''  # Initializes the sub_dir variable (needed even if no sub directory is used)

    if sub_dir ==  '':
        sub_dir=sub_dir+'/'

    fileList = cfg.fileList # Add more files like this: ('CONVqn.xlsx','AnotherFile.dat')
    reffile = cfg.reffile # 'water_output.gdx'

    downList=cfg.downList

    root_url = cfg.root_url  # 'https://news-link.environmentalcrossroads.org/'
    glob_url = cfg.glob_url  # root_url + 'news_transfer/'                        # URL
    cgi_url = cfg.cgi_url  # root_url + 'cgi-bin/put_data.py'                # Upload script
    dir_up = cfg.dir_up  # 'ReEDS/'                # DO NOT CHANGE                # Remote dir for the data
    dir_down = cfg.dir_down # 'TP2M/'
    log_file = cfg.log_file # 'status.txt'

    user = cfg.user # User name
    password = cfg.password # Hashed password

    #######################################################################

    main() # sys.argv)
    sys.exit()
