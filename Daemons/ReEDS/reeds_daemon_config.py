#!/usr/bin/python

'''
reeds_daemon_config.py

NEWS http link configuration file
Defines the main variables shared by the various routines
Import in the routine files as:
    import news_transfer_config as cfg
'''

################# User name and hashed password #################

user = 'newswebuser'
password = 'xxxx' # INSERT HASHED PASSWORD HERE

################# Remote server access variables (leave as is) #################

# Remote server root URL
root_url='https://news-link.environmentalcrossroads.org/'
# Server local paths
server_htdocs = '/var/www/news_link/news_transfer/'
# Web site root directory for project
web_rundir = '/news_transfer/'
# Full URL to transefer site
glob_url = root_url + web_rundir
# CGI-BIN folder and server=side script
cgi_url = root_url + 'cgi-bin/put_data.py'
# dir to store uploaded on server (note the slash at the end of the string)
dir_up = 'ReEDS/'
# dir that stores files to download (note the slash at the end of the string)
dir_down = 'TP2M/'
# Status (log) files on the link server (one in each of the dir_up and dir_down folders)
log_file = 'status.txt'

################## Local varibles that may need to be changed ##################

# Upload data dir - directory monitored by the daemon for new ReEDS results
data_dir_upload = 'X:/FY17-NEWS-SMC/Full 60 scenarios/201705_round3/scenarios_NoCPP/'

# Download data dir - directory to store data download from the link server
data_dir_down = 'C:/Users/fabio/Documents/NEWS/ReEDS_Test/'

# Subdirectory to upload (note the slash at the end of the string)
# the sub_dir variable is needed even if no sub directory is used (set to '')
sub_dir = 'gdxfiles/'

# List of files to upload (if present), everything else in the subdirectory will be ignored
fileList  = ('water_output.gdx','reporting.gdx')

# List of files to download from web server
downList  = ('aac_mean.csv','aac_min.csv')

# Upload time reference file: during a looping experiment, the timestamp of this file will be used to
# check if a new upload (e.g. a new iteration) is available (must be included in the list above)
reffile = 'water_output.gdx'
