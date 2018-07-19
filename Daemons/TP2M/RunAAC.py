#! /usr/bin/env python3
#

"""
Service RunAAC.py
Monitors Log folder, gets list of model runs that have completed
succesfully and queues for the next step of processing
"""

import glob,os,subprocess,sys,time,argparse
from argparse import RawTextHelpFormatter

d={'prime':'prime',
   'AAC':'reeds'}

########################################################################
###################         Functions   ###############################
def RunScript(ScriptToProcess,ProcessType):

    cmd = ['/asrc/ecr/NEWS/configurations/Link/MainLink.py']
    if ProcessType == 'All':
        Processes=['prime','AAC']
    else:
        Processes =[ProcessType]
    for Process in Processes:
        print_if('Starting {} processing on file {}'.format(Process, ScriptToProcess))
        scriptFile=ScriptFolder + '/' + ScriptToProcess + '.sh'
        logFile=LogFolder + '/' + ScriptToProcess + '.log'
        cmd2 = [scriptFile, '-V', '-t', d[Process], '-l', logFile]
        subprocess.call(cmd + cmd2) # , stdout=f, stderr=f)
        print_if('Finished {} processing on file {}'.format(Process, ScriptToProcess))
    return

def print_if(value):
    if print_if_flag:
        print(value)
        sys.stdout.flush()

def tail(filepath):
    """
    @author Marco Sulla (marcosullaroma@gmail.com)
    @date May 31, 2016
    """

    try:
        filepath.is_file
        fp = str(filepath)
    except AttributeError:
        fp = filepath

    with open(fp, "rb") as f:
        size = os.stat(fp).st_size
        start_pos = 0 if size - 1 < 0 else size - 1

        if start_pos != 0:
            f.seek(start_pos)
            char = f.read(1)

            if char == b"\n":
                start_pos -= 1
                f.seek(start_pos)

            if start_pos == 0:
                f.seek(start_pos)
            else:
                char = ""

                for pos in range(start_pos, -1, -1):
                    f.seek(pos)

                    char = f.read(1)

                    if char == b"\n":
                        break

        return f.readline()

#######################################################################
##############   Check for script and execute   #############
def main():
    try:
        while (1):
            ### Build list of directories to download
            #Scripts = glob.glob(ScriptFolder + "/WBM_TP2M*.sh")
            Logs = glob.glob(LogFolder + "/WBM_TP2M*.log")
            ToProcess = []
            for Log in Logs:
                if 'Model run finished:' in str(tail(Log)):
                    #print('Model {} finished succesfully'.format(Log))
                    ToProcess.append(os.path.basename(Log.replace('.log', '')))
            #print('pippo')
            #S = list((os.path.basename(os.path.splitext(x)[0]) for x in Scripts))
            #L = list((os.path.basename(os.path.splitext(x)[0]) for x in Logs))

            #ToProcess = list((set(S) ^ set(L)) & set(S))

            if len(ToProcess)>0:
                RunScript(ToProcess[0],action)
            else:
                print_if('Nothing new to process in folder {}'.format(ScriptFolder))
                time.sleep(cycle_min * 60)        ### Sleep time: converts minutes to seconds
            #    print_if('Checking script folder {} for new runs to process'.format(ScriptFolder))
    except KeyboardInterrupt:
        print('\nKeyboardinterrupt: ...Program Stopped Manually!')
        sys.exit()

if __name__ == "__main__":

    """""
    main
    """""
    #############################################################################
    ##################        Parameters to modify        #######################

    parser = argparse.ArgumentParser(description='Allocates ReEDS output to a WBM network',
                                             formatter_class=RawTextHelpFormatter)

    #parser.add_argument('-b', '--base_folder',
    parser.add_argument(dest='MonitorFolder',
                        help="Base directory of TP2M project",
                        default="/asrc/ecr/NEWS/MultiScenario")

    #parser.add_argument('-a', '--action',
    parser.add_argument(dest='action',
                        choices=['prime', 'AAC', 'All'],
                        help="Only perform the specified task: priming, AAC calculation or both (depending on the type of model run)",
                        default="")

    parser.add_argument('-s', '--scripts',
                                dest='ScriptFolder',
                                help="Scripts folder within base directory (default=Scripts)",
                                default='Scripts')

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

    print_if_flag = args.print_if_flag #1
    cycle_min = int(args.cycle_min) # 10  ### Time interval in minutes between directory status re-checks

    action = args.action

    main()



print("end")

