#! /usr/bin/env python3
#

"""
R2T_getreedsinput.py
"""

from bs4 import BeautifulSoup
import requests
import paramiko
from scp import SCPClient
import time
import calendar
import pandas as pd
import subprocess

import R2T_Globals
from R2T_Globals import *

#from datetime import datetime


# Size file checks to make sure the file son the server are correct
FormatChecks={'CSV':1100000,
              'Excel':17000000,
              'GDX':500000
              }
# Files to check size and time stamp for each one of the possible
# file formats
FormatFiles={'CSV':'convqctn.csv',
              'Excel':'Water_output.xlsx',
              'GDX':'water_output.gdx'
              }


def Connect():
    # This routine extablishes the SSH conneciton with the server
    # hosting the web link and the uploaded files
    #
    # returns an ssh connection
    #
    print_if('Establishing SSH connection')
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(R2T_Globals.LinkServer) #('sneezy.ccny.cuny.edu') #, username=user, password=password)
    return ssh

def CheckReEDS_fileFormat(list):
    # This routine checks which file formats we have on the server
    # Preference is give to the csv file (checked first in the sequence)
    #
    # Input is directory listing on server
    #
    # Returns:  item in directory listing with the key file
    #           flaq (excel_flag) set to 'CSV', 'Excel, or 'GDX'
    rec = [s for s in list if "CONVqctn.csv".lower() in s]
    if len(rec) > 0:
        excel_flag='CSV'#False
    else:
        rec = [s for s in list if "Water_output.xlsx.".lower() in s]
        if len(rec) > 0:
            excel_flag = 'Excel' # True
        else:
            rec = [s for s in list if "water_output.gdx" in s]
            if len(rec) > 0:
                excel_flag = 'GDX'  # True
            else:
                excel_flag = 'Error' # False
    return rec,excel_flag

def GetReEDS(ssh,StartDate='2018-01-01'):
    # This routine gets the list of files to be downloaded from the server
    # Uses the ssh connection established be the routine Connect()
    # If specificed, uses a filter to only work on files more recent than StartDate
    #
    # Calls routine Translate2TP2M to translate faile naming conventions from ReEDS to TP2M
    #
    # Returns dataframe of candidate files to be uploaded
    #

    print_if('Retrieving ReEDS simulation results list from Link server')
    #cmd_to_execute='ls --full-time /var/www/news_link/news_transfer/ReEDS' #/NorESM1_M_RCP8p5_CAP'
    cmd_to_execute='find '+ R2T_Globals.LinkServerReEDS + ' -maxdepth 1 -newermt "' + StartDate + '" -type d'
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
    cols=['TimeStamp','DirectoryName','FileSize','CheckFile','Excel']
    ReEDS_input=pd.DataFrame(columns=cols)
    i=0
    lines=ssh_stdout
    lines=list(lines)[1:]
    for line in lines:
        #if line[0]=='d':
            items=pd.Series(name=str(i))
            i += 1
            #item=line.strip('\n').split()
            #item=line.strip('\n').lstrip('/var/www/news_link/news_transfer/ReEDS/')
            item=os.path.split(line)[1].strip('\n')
            items['DirectoryName']=item #[8]
            cmd_to_execute = 'ls --full-time -R ' + R2T_Globals.LinkServerReEDS + '/'+items['DirectoryName']  # + '/Water_output.xlsx'  # /NorESM1_M_RCP8p5_CAP'
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
            l,items['Excel']=CheckReEDS_fileFormat(ssh_stdout.readlines())
#            matching = [s for s in l if "CONVqctn.csv" in s]
#            CheckReEDS_fileFormat(l)
            if len(l)>0:
                #if items['Excel']:
                #    CheckSize=17000000
                #else:
                #    CheckSize=1100000
                item=l[0].strip('\n').split()
                items['TimeStamp'] = item[5] + 'T' + item[6][:-3] + item[7]
                if item[4] == item[4]:
                    items['FileSize'] = int(item[4])
                else:
                    items['FileSize'] = 0
                if items['FileSize'] < FormatChecks[items['Excel']]: #CheckSize:
                    items['CheckFile'] = True
                else:
                    items['CheckFile'] = False
            ReEDS_input=ReEDS_input.append(items)
    ReEDS_input['TimeStamp']=pd.to_datetime(ReEDS_input['TimeStamp'],utc=True)
    ReEDS_input=ReEDS_input.loc[ReEDS_input['TimeStamp']>pd.to_datetime(StartDate)]
    return Translate2TP2M(ReEDS_input)
#     ReEDS_input #.apply(lambda x: x.strftime('%Y-%m-%d')))

def Translate2TP2M(FileList):
    # Here we translate the ReEDS output file name to the entries used to
    # create the directory structure for the TP2M analysis and the name of
    # the script that will run TP2M
    #
    # Note that the NoClimate scenarios, will be analysed in TP2M with all
    # the GCMs and all the RCPs. Thus we need to create entries for all
    # these analyses.
    #
    # For all the NoClimate the ReEDS to TP2M results will be the same for all
    # GCPs and RCPs, so the actual calculations are done once.
    # Symbolic links are used to point to the TP2M input for all the other
    # folders
    # We use the 'symlink' flag in the file list to indicate that no processing
    # is needed and that a symlink will be created once the main folder is processed

    OutList=FileList.copy()
    # We add the needed columns to the data frame
    OutList['gcm']=''
    OutList['rcp']=''
    OutList['scenario']=''
    OutList['qualifier']=''
    OutList['path']=''
    OutList['SN_gcm']=''
    OutList['SN_rcp']=''
    OutList['script']=''
    OutList['symlink']=False
    # And now we load the information using the Excel translation table
    # (e.g. file /asrc/ecr/NEWS/configurations/Link/ReEDS_TP2M_LookupsActive.xlsx, sheet ModelsCodes)
    #
    for index,row in FileList.iterrows():
        name=row['DirectoryName']
        for i,dict in R2T_Globals.NamingConvention.iterrows():
            if dict['reeds'] in name:
                item=dict['type']
                value=dict['tp2m']
                OutList.loc[index,item]=value
                if item in ['gcm','rcp']:
                    item='SN_'+item
                    value=dict['script']
                    OutList.loc[index, item] = value

    # We now extract all the GCMs and the RCPs to generate all the entries for the NoClimate
    tmp=R2T_Globals.NamingConvention.loc[R2T_Globals.NamingConvention['type']=='gcm',['tp2m','script']]
    gcms=list(tmp['tp2m']+'^'+tmp['script'])
    tmp=R2T_Globals.NamingConvention.loc[R2T_Globals.NamingConvention['type']=='rcp',['tp2m','script']]
    rcps=list(tmp['tp2m']+'^'+tmp['script'].astype(str))

    # We fisrt check if we have entries without both gcm AND rcp. If that is the case
    # we assume taht this is a NoClimate spinup, and generate the corresponding entries
    # for each CGM and ech RCP
    NoClimate=OutList.query('gcm == "" and rcp == ""')
    #OutList.loc[ OutList['gcm']=='' & OutList['rcp']=='']

    if len(NoClimate) > 0:
        AddRows=pd.DataFrame()
        # Here we iterate all the NoClimate and we generate entries for all the gcms and all the rcps
        for index, row in NoClimate.iterrows():
            for g in gcms:
                g_d,g_s=g.split('^')
                for r in rcps:
                    r_d, r_s = r.split('^')
                    tmpDF = row.copy()
                    tmpDF['gcm']=g_d
                    tmpDF['rcp']=r_d
                    tmpDF['SN_gcm']=g_s
                    tmpDF['SN_rcp']=r_s
                    AddRows=AddRows.append(tmpDF, ignore_index=True)

        # We filter out the original NoClimate entries
        OutList=OutList.loc[OutList['gcm'] != '']
        # And add the newly created ones in the step above
        OutList=pd.concat([OutList,AddRows],axis=0).reset_index()

    # Now we check if we have no GCM, if so, we assume we are working on the GCM averages and
    # we create the TP2M input file under the hadgem2 GCM and then symlink all the other
    GCMAverages=OutList.query('gcm == ""')
    if len(GCMAverages) > 0:
        cols=GCMAverages.columns
        AddRows = pd.DataFrame(index=range(0,len(GCMAverages)*len(gcms)),columns=cols)
        # We iterate through all the records in the GCMAverages and create
        # an entry for each GCM
        i=0
        for index, row in GCMAverages.iterrows():
            for g in gcms:
                g_d,g_s=g.split('^')
                for col in GCMAverages.columns:
                    AddRows.loc[(AddRows.index == i), col] = row[col]
                #tmpDF = row.copy()
                #tmpDF = row.to_dict()
                #tmpDF['gcm'] = g_d
                AddRows.loc[(AddRows.index==i),'gcm'] = g_d
                #tmpDF['SN_gcm'] = g_s
                AddRows.loc[(AddRows.index==i),'SN_gcm'] = g_s
                #if tmpDF['gcm'] == 'hadgem2-es':
                #    tmpDF['symlink'] = False
                #else:
                #    tmpDF['symlink'] = True
                # AddRows.loc[(AddRows.index==i),'gcm']
                if g_d == 'hadgem2-es':
                    AddRows.loc[(AddRows.index == i),'symlink'] = False
                else:
                    AddRows.loc[(AddRows.index == i),'symlink'] = True
                i+=1
                #print(type(tmpDF))
                #AddRows = pd.concat([AddRows,tmpDF],axis=1) #AddRows.append(tmpDF, ignore_index=True)
        # We filter out the original NoClimate entries
        OutList=OutList.loc[OutList['gcm'] != '']
        # And add the newly created ones in the step above
        OutList=pd.concat([OutList,AddRows],axis=0).reset_index()

    # WE are ready to create the base path for the analysis and the name of the
    # WBM script that will run the TP2M
    OutList.loc[ OutList['qualifier']=='' ,'path'] = R2T_Globals.ProjectBase+OutList['gcm']\
                                                   +'/'+OutList['rcp']\
                                                   +'/'+OutList['scenario']
    OutList.loc[OutList['qualifier'] == '', 'script'] = R2T_Globals.ScriptBase + OutList['SN_gcm']\
                                                    + OutList['SN_rcp'].astype(int).astype(str) + '_' \
                                                    + OutList['scenario']

    OutList.loc[ OutList['qualifier']!='' ,'path']=R2T_Globals.ProjectBase+OutList['gcm']\
                                                   +'/'+OutList['rcp']\
                                                   +'/'+OutList['qualifier']\
                                                   +'_'+OutList['scenario']
    # The lines above use the underscore as separator between the nc (NoClimate) qualifier
    # and the scenario. The lines commented below changed this to the - to avoid conflicts
    # in the second part of the processing
    # Note that the same change would need to be done further down in the createScript function
    # Problem superseeded directly in the processing down the road. the change would have been
    # much more massive than originally anticipated

    #OutList.loc[ OutList['qualifier']!='' ,'path']=R2T_Globals.ProjectBase+OutList['gcm']\
    #                                               +'/'+OutList['rcp']\
    #                                               +'/'+OutList['qualifier']\
    #                                               +'-'+OutList['scenario']
    OutList.loc[OutList['qualifier'] != '', 'script'] = R2T_Globals.ScriptBase + OutList['SN_gcm']\
                                                    + OutList['SN_rcp'].astype(int).astype(str) \
                                                    + '_' + OutList['qualifier'] \
                                                    + '_' + OutList['scenario']
    # Finally wwe set the symlink flag for those GCMs and RCPs for which the ReEDS input is NoClimate
    # except for the hadgem2-es CGM that is our reference CGM to which all others are symlinked
    OutList.loc[(OutList['qualifier'] != '') & (OutList['gcm'] != 'hadgem2-es'),'symlink']=True

    return OutList

def CheckNew(FileList):
    # Now we can check if the files loaded from the Link server are new or have already
    # been processed.
    #
    # Check is based on the time stamp that si syncronized from the server
    #
    # We also finalize the path name by adding the version (used in looping)

    print_if('Checking for new ReEDS simulations to download from Link server')
    FileList['new']=True
    FileList['version']=''
    for index, row in FileList.iterrows():
        CurrPath=row['path']
        version = 'v000'
        if os.path.exists(CurrPath):
            versions=os.listdir(CurrPath)
            if len(versions) > 0:
                version=max(versions)
                #FileList.loc[index, 'version']=version
                #if row['Excel']:
                    #FileName=CurrPath+'/'+version+'/ReEDS2TP2M/ReEDSresults/Water_output.xlsx'
                #else:
                    #FileName=CurrPath+'/'+version+'/ReEDS2TP2M/ReEDSresults/convqctn.csv'
                FileName = CurrPath + '/' + version + '/ReEDS2TP2M/ReEDSresults/'+FormatFiles[row['Excel']]
                if os.path.isfile(FileName):
                    # check timestamp of ReEDS file with saved one
                    existingFile=os.path.getmtime(FileName)
                    # check if it is within 0.000001 from the time on the server
                    #timeDiff=time.mktime(row['TimeStamp'].timetuple()) + row['TimeStamp'].microsecond * 0.000001 - existingFile
                    timeDiff=calendar.timegm(row['TimeStamp'].timetuple()) + row['TimeStamp'].microsecond * 0.000001 - existingFile
                    if abs(timeDiff) < 0.000001:
                        FileList.loc[index, 'new'] = False
                    else:
                        if timeDiff < 0:
                            print('timestamp of file {} newer than version on link server. Assuming no new version available'.format(FileName))
                            FileList.loc[index, 'new'] = False
                        else:
                            verNum = int(version[1:]) + 1
                            version = 'v' + '{0:03d}'.format(verNum)
        FileList.loc[index, 'version']=version

    return FileList #.loc[FileList['new']]

def createDirStruct(ToCreate):
    # Based on the results of the above routines we create the directory
    # structure that will accomodate all the TP2M runs
    #
    # for the no_climate scenarios we only convert the ReEDS output once
    # and then we create symbolic links to the folder where they will be created
    # For tradition we use hadgem as our "active" gcm and symlink the rest

    #ToCreate=FileList.copy()
    print_if('Creating directory structure')
    for index, row in ToCreate.sort_values('symlink').iterrows():
        #if row['version'] != '':
        #    verNum=int(row['version'][1:])+1
        #else:
        #    verNum=0
        #version='v'+'{0:03d}'.format(verNum)
        #ToCreate.loc[index,'version']=version
        version=row['version']
        if row['symlink']:
            dst=row['path']+'/'+version + '/ReEDS2TP2M'
            if not os.path.islink(dst):
                src=dst.replace(row['gcm'],'hadgem2-es')
                make_sure_path_exists(src)
                make_sure_path_exists(row['path'] + '/' + version)
                os.symlink(src, dst)
        else:
            for d in ['ReEDSresults','TP2Minput']:
                path=row['path']+'/'+version+'/ReEDS2TP2M/'+d
                make_sure_path_exists(path)
    return

def getLog(ssh):
    # We get the website status file so we can update it as we download the files
    sftp_client = ssh.open_sftp()
    remote_file = sftp_client.open(R2T_Globals.LinkServerReEDS + '/status.txt', 'r')
    try:
        lines =remote_file.readlines()
    finally:
        remote_file.close()
    log_list = [rec.rstrip("\n").split("\t") for rec in lines]
    return log_list

def setLog(log_list,ssh):
    # We set the website status file with the updated info for the downloaded files
    log_str = "\n".join("\t".join(rec) for rec in log_list) + "\n"

    cmd_to_execute ='/usr/local/share/news/ssh_access.py "' + log_str +\
        '" -l status.txt -d ReEDS/ -t US/Eastern -c http://localhost/'+ R2T_Globals.LinkServerLocalCGI + 'put_data.py'
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)

    if ssh_stderr.channel.recv_exit_status() == 0:
        print_if('Succesfully updated the Link Server log file')

    #sftp_client = ssh.open_sftp()
    #remote_file = sftp_client.open(R2T_Globals.LinkServerReEDS + '/status.txt', 'w')
    #try:
    #    remote_file.write(log_str)
    #finally:
    #    remote_file.close()
    return

def getData(FileList,ssh):

    print_if('Downloading {} new ReEDS simulations from link server'.format(len(FileList.index)))
    n=len(FileList.index)
    scp=SCPClient(ssh.get_transport())
    log_list=getLog(ssh)
    for index,row in FileList.iterrows():
        print('\r {} Downloading simulation {}        '.format(n*-1, row['DirectoryName']), end='', flush=True)
        n -= 1

        if row['Excel']=='Excel':
            DownLoads=pd.DataFrame(columns=['ServerSide','ModelSide'],index=[0])
            DownLoads['ServerSide']=R2T_Globals.LinkServerReEDS + '/'+row['DirectoryName']+'/Water_output.xlsx'
            DownLoads['ModelSide']=row['path']+'/'+row['version']+'/ReEDS2TP2M/ReEDSresults/Water_output.xlsx'
        else:
            if row['Excel'] == 'GDX':
                nf=list(range(0,len(R2T_Globals.ReEDS_files_GDX)))
                DownLoads = pd.DataFrame(columns=['ServerSide', 'ModelSide'], index=nf)
                for i, f in DownLoads.iterrows():
                    DownLoads.loc[DownLoads.index == i,'ServerSide'] = R2T_Globals.LinkServerReEDS + '/' + row[
                        'DirectoryName'] + '/gdxfiles/' + R2T_Globals.ReEDS_files_GDX[i]
                    DownLoads.loc[DownLoads.index == i,'ModelSide'] = row['path'] + '/' + row[
                        'version'] + '/ReEDS2TP2M/ReEDSresults/' + R2T_Globals.ReEDS_files_GDX[i]
            else:
                nf=list(range(0,len(R2T_Globals.ReEDS_files)))
                DownLoads = pd.DataFrame(columns=['ServerSide', 'ModelSide'], index=nf)
                for i, f in DownLoads.iterrows():
                    DownLoads.loc[DownLoads.index == i,'ServerSide'] = R2T_Globals.LinkServerReEDS + '/' + row[
                        'DirectoryName'] + '/wateroutput/' + R2T_Globals.ReEDS_files[i]
                    DownLoads.loc[DownLoads.index == i,'ModelSide'] = row['path'] + '/' + row[
                        'version'] + '/ReEDS2TP2M/ReEDSresults/'  + R2T_Globals.ReEDS_files[i]

        # We set the Download flag in the link server log file
        for rec in log_list:
            if rec[0]==row['DirectoryName']:
                rec[5]=str(1)
                break

        # We copy the files from the Link server to the local directory structure
        for i, f in DownLoads.iterrows():
            scp.get(f['ServerSide'],f['ModelSide'])
            if len(DownLoads.index) == 1 or \
                os.path.basename(f['ModelSide']) == 'convqctn.csv' or \
                os.path.basename(f['ModelSide']) == 'water_output.gdx':
                # Set the time stamp to the same time stamp of the file on the server
                # this is done for the file:
                #    water_output.xlsx --> when the Excel field is set to "Excel"
                #    convqctn.csv --> when the Excel field is set to "CSV"
                #    'water_output.gdx' --> when the Excel field is set to "GDX"
                # If other files are present in the folder, their timestamp is not modified
                timeStampServer=calendar.timegm(row['TimeStamp'].timetuple()) + row['TimeStamp'].microsecond * 0.000001
                os.utime(f['ModelSide'],(timeStampServer,timeStampServer))
            # And finaly we set the access rights
            os.chmod(f['ModelSide'], 0o664)
    # We now write bach the link server status file
    #print('\r {}'.format(" " * 80), flush=True)
    print('\rDone downloading new ReEDS simulations results from Link server                ', flush=True)
    setLog(log_list,ssh)


def createScripts(FileList,template='WBM_TP2M_template'):
    print_if('Creating WBM TP2M run scripts')
    for index,row in FileList.iterrows():
        createScript(row,template)

def createScript(row,template='WBM_TP2M_template'):
    inFile=R2T_Globals.ProjectHome+'/Environment/'+template
    if row['version'] != '':
        outFile=R2T_Globals.ProjectBase+'/Scripts/'+row['script'] + '_' + row['version'] + '.sh'
    else:
        outFile = R2T_Globals.ProjectBase + '/Scripts/' + row['script'] + '.sh'

    # Make sure that the Scripts folder and the Logs folder exists in the R2T_Globals.ProjectBase
    # folder...
    make_sure_path_exists(R2T_Globals.ProjectBase + '/Scripts/')
    make_sure_path_exists(R2T_Globals.ProjectBase + '/Logs/')

    fileOut=open(outFile,'w')
    with open(inFile) as openfileobject:
        for line in openfileobject:
            if 'EXPERIMENT=' in line: #!!!! #"nc_bau" # Either ReEDS priming or ReEDS scenario for looping
                value=row['scenario']
                if row['qualifier'] !='':
                    value='nc_'+value
                    #value='nc-'+value
            elif 'RUNVER=' in line: #!!!! #"v0"
                value = row['version']
            elif 'GCM=' in line: #!!!! #"hadgem2-es"
                value = row['gcm']
            elif 'RCP=' in line: #!!!! #"rcp8p5"
                value = row['rcp']
            else:
                value='NOTHING TO DO'
            if value=='NOTHING TO DO':
                fileOut.write(line)
            else:
                fileOut.write(line.replace('!!!!','"' + value + '"'))
    fileOut.close()


def runs():
    connection=Connect()
    ReEDSfiles=GetReEDS(connection)
    #ReEDSfiles['version']='v000'
    #getData(ReEDSfiles.loc[ReEDSfiles['symlink'] != True], connection)
    List=CheckNew(ReEDSfiles)
    if len(List.loc[List['new']].index) > 0:
        ReEDSfiles=List.loc[List['new']]
#        createScripts(ReEDSfiles)
        createDirStruct(ReEDSfiles)
        getData(ReEDSfiles.loc[ReEDSfiles['symlink'] != True],connection)
    else:
        print('No new ReEDS simulations on Link server')
    connection.close()
    List['unimported']=False
    List['unmodeled']=False
    List['unscripted']=False
    for index,row in List.loc[List['symlink']!=True].iterrows():
        if os.listdir(row['path'] + '/' + row['version'] + '/ReEDS2TP2M/TP2Minput/') == []:
            List.loc[index,'unimported']=True
    for index, row in List.iterrows():
        if not os.path.exists(row['path'] + '/' + row['version'] + '/RGISresults/'):
            List.loc[index,'unmodeled']=True
        if row['version'] != '':
            outFile = R2T_Globals.ProjectBase + '/Scripts/' + row['script'] + '_' + row['version'] + '.sh'
        else:
            outFile = R2T_Globals.ProjectBase + '/Scripts/' + row['script'] + '.sh'
        if not os.path.exists(outFile):
            List.loc[index,'unscripted']=True

    return List

if __name__ == "__main__":

    init()
    R2T_Globals.print_if_flag=True
    R2T_Globals.LinkServerReEDS = '/Users/ecr/fabio/Sites/ReEDS_Daemon/html/ReEDS'
    R2T_Globals.ProjectBase='/asrc/ecr/NEWS/tmp/'
    R2T_Globals.ProjectHome='/asrc/ecr/NEWS/LoopingPaper/'
    R2T_Globals.LinkServerLocalCGI='~fabio/ReEDS_Daemon/cgi-bin/'
    ReEDSfiles=runs()
#    print(ReEDSfiles[['DirectoryName','gcm','rcp','scenario','qualifier','path','new','version']])