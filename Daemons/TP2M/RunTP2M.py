#! /usr/bin/env python3
#

"""
Service RunTP2M.py
Monitors Script folder, gets list of scripts and queues for execution
the ones that don't have a corresponding log file in the Logs folder
"""

import glob,os,subprocess,sys,time,argparse
from argparse import RawTextHelpFormatter


########################################################################
###################         Functions   ###############################
def RunScript(ToProcess):
    cmd=[ScriptFolder + '/' + ToProcess + '.sh', '-V']

    with open(LogFolder + '/' + ToProcess + '.log', 'w') as f:
        return subprocess.call(cmd, stdout=f, stderr=f)

def print_if(value):
    if print_if_flag:
        print(value)
        sys.stdout.flush()

#######################################################################
##############   Check for script and execute   #############
def main():
    try:
        while (1):
            ### Build list of directories to download
            Scripts = glob.glob(ScriptFolder + "/WBM_TP2M*.sh")
            Logs = glob.glob(LogFolder + "/WBM_TP2M*.log")

            S = list((os.path.basename(os.path.splitext(x)[0]) for x in Scripts))
            L = list((os.path.basename(os.path.splitext(x)[0]) for x in Logs))

            ToProcess = list((set(S) ^ set(L)) & set(S))

            if len(ToProcess)>0:
                print_if('Running TP2M on script {}'.format(ToProcess[0]))
                RunScript(ToProcess[0])
                print_if('Finished running TP2M script {}'.format(ToProcess[0]))
            else:
                print_if('Nothing new to process in folder {}'.format(ScriptFolder))
                time.sleep(cycle_min * 60)        ### Sleep time: converts minutes to seconds
                print_if('Checking script folder {} for new runs to process'.format(ScriptFolder))
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

    parser.add_argument('-b', '--base_folder',
                                dest='MonitorFolder',
                                help="Base directory of TP2M project",
                                default="/asrc/ecr/NEWS/LoopingPaper")

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

    main()



print("end")

