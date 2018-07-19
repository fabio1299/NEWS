#!/usr/bin/python

# Import modules for CGI handling
import cgi, cgitb
import os, shutil
import news_transfer_config as cfg

cgitb.enable()

print("Content-type: text/html\r\n\r\n")

#######################################################################
######################  Functions  ####################################

def getLastRecords(file, n_records):  ### Read Last records in a log file

    with open(file, 'r') as f:
        records = f.readlines()


    if n_records > len(records):
        n_records = len(records)

    tmp=records[0:n_records]
    tmp.reverse()
    return tmp

#########################################################################

fs = cgi.FieldStorage()

#######################################################################
##################   Read Arguments/Parameters   ######################
if fs.has_key('set'):
    set = fs['set'].value
else:
    set = 'TP2M'
if fs.has_key('remove'):
    rm_name = fs['remove'].value
    if rm_name == '0':
        rm_name = 0
else:
    rm_name = 0
if fs.has_key('newname'):
    newname = fs['newname'].value  # ||  0;
    if newname == '0':
        newname = 0
else:
    newname = 0
if fs.has_key('move'):
    move = int(fs['move'].value)  # ||  0;
else:
    move = 0
if fs.has_key('list'):
    list = fs['list'].value  # ||  0;
    if list == '0':
        list = 0
else:
    list = 0

# print set, rm_name, newname, move, list
#######################################################################
####################### Set global variables ##########################
htdocs = cfg.server_htdocs # '/Users/ecr/fabio/Sites/ReEDS_Daemon/html/'
# htdocs='/var/www/news_link'
# my $htdocs	= '/net/nfs/merrimack/web/earthatlas/htdocs';
run_dir = cfg.web_rundir + set # "~fabio/ReEDS_Daemon/html/" + set
log_file = htdocs + set + '/status.txt'  # $htdocs.$run_dir.'status.txt';

#######################################################################

##################   Remove/Rename Record   ###########################
if rm_name != 0:
    # print('Inside if')
    ### Rename/Remove directories
    mysrc = htdocs + set + '/' + rm_name
    if newname != 0:
        # rename $htdocs.$run_dir.$rm_name, $htdocs.$run_dir.$newname;
        mydst = htdocs + set + '/' + newname
        os.rename(mysrc, mydst)
    else:
        # rmtree $htdocs.$run_dir.$rm_name;
        if os.path.isdir(mysrc):
            shutil.rmtree(mysrc)
        else:
            os.remove(mysrc)

    ### Update Log file
    log = ''
    with open(log_file, 'rt') as file:
        if newname != 0:
            log = file.read().replace(rm_name + '\t', newname + '\t')
        else:
            for line in file.readlines():
                if line.split('\t')[0] != rm_name:
                    log=log+line
    with open(log_file, 'wt') as file:
        file.write(log)

#######################################################################
##################   Move a recond Up/Down   ##########################

if move != 0:  # {
    # open (FILE,"<$log_file") or die "Couldn't open $log_file, $!";
    # my @lines = <FILE>;
    # close FILE;
    with open(log_file, 'rt') as file:
        lines = file.readlines()

    i = 0
    for l in lines:
        if newname + '\t' in l:
            break
        i += 1

    if move == 1:
        lines[i:i + 2] = reversed(lines[i:i + 2])
    elif move == -1:
        lines[i - 1:i + 1] = reversed(lines[i - 1:i + 1])

    #for line in lines:
    #    if newname + '\t' in line:
    #        newpos = counter + move
    #        if newpos > 0:
    #            tmp = line
    #            lines[counter] = lines[newpos]
    #            lines[newpos] = tmp
    #    counter += 1

    with open(log_file, 'wt') as file:
        file.write("".join(lines))

    # my $ind = (grep {$lines[$_]=~m/^$newname\t/} 0..$#lines)[0];
    #    if (defined($ind) && (($move == -1 && $ind != 0) || ($move == 1 && $ind != $#lines))) {
    #        splice(@lines,$ind+$move,0,splice(@lines,$ind,1));
    #
    #        open (FILE,">$log_file") or die "Couldn't open $log_file, $!";
    #        print FILE @lines;
    #        close FILE;
    #    }
# }

#######################################################################
#################   Get list of files in subfolder   ##################

list_html = ''

if list != 0:  # ) {
    list_dir = htdocs + set + '/' + list  # $htdocs.$run_dir.$list;
    files = os.listdir(list_dir)
    # opendir (DIR, $list_dir) || die "Can't opendir $list_dir: $!";
    # my @files	= sort grep(m/^\w/,readdir(DIR));
    # foreach my $file (@files) {
    for file in files:
        full_path=run_dir + '/' + list + '/' + file
        if os.path.isdir(full_path):
            list_html = list_html + \
                        '<a href=\"' + \
                        run_dir + '/' + \
                        list + '/' + file + \
                        '\" style=\"font-weight: bold; color: blue; text-decoration: none\">' + \
                        file + '</a><br />\n'
        else:
            list_html = list_html + \
                    '<a href=\"' + \
                    run_dir + '/' + \
                    list + '/' + file + \
                    '\" style=\"font-weight: bold; color: blue; text-decoration: none\">' + \
                    file + '</a><br />\n'
        # $list_html .= "<a href=\"$run_dir$list/$file\" style=\"font-weight: bold; color: blue; text-decoration: none\">$file</a><br />\n";
        # }
    # closedir DIR;
# }

#######################################################################
######################  Read Data  ####################################

records=[]

if list == 0:
    records = getLastRecords(log_file, 200)  # unless $list;

#######################################################################
######################   Make HTML  ###################################

html = ('''\
<table class="table_top">
  <tr>
    <th class="cell_header_blue" colspan="6"><span style="font-size:larger;">_SET_</span>
	<a href="" class="refresh-log">(Refresh Now:</a> Auto-refresh interval is 1 hour)</th>
  </tr>
  <tr>
    <th style="width: 35%" class="cell_header_blue">Simulation ID</th>
    <th style="width:  5%" class="cell_header_blue">Move</th>
    <th style="width: 10%" class="cell_header_blue">Action</th>
    <th style="width: 15%" class="cell_header_blue">Date Uploaded</th>
    <th style="width: 15%" class="cell_header_blue">IP</th>
    <th style="width: 20%" class="cell_header_blue">Host</th>
  </tr>
_ROWS_
</table>
''')

row_template = ('''\
  <tr>
    <td style="width: 35%" class="cell_normal_blue _SIM_ID_">
      <a href="" class="listSet" simID="_SIM_ID_">_SIM_ID_</a>
    </td>
    <td style="width:  5%" class="cell_normal_blue _SIM_ID_">
      <a href="" class="moveSet" simID="_SIM_ID_" action="1">
	<img alt="Move up" height="16" width="16"  class="no_border" src="images/arrow_up.png" /></a>
      <a href="" class="moveSet" simID="_SIM_ID_" action="-1">
	<img alt="Move down" height="16" width="16"  class="no_border" src="images/arrow_down.png" /></a>
    </td>
    <td style="width: 10%" class="cell_normal_blue _SIM_ID_" style="text-align: center;">
      <a href="" class="renameSet" simID="_SIM_ID_">Rename</a> |
      <a href="" class="removeSet" simID="_SIM_ID_">Remove</a>
    </td>
    <td style="width: 15%" class="cell_normal_blue _SIM_ID_">_DATE_</td>
    <td style="width: 15%" class="cell_normal_blue _SIM_ID_">_IP_</td>
    <td style="width: 20%" class="cell_normal_blue _SIM_ID_">_HOST_</td>
  </tr>
''')

rows = ''
for record in records:
    # {
    items = record.split('\t')
    one = row_template
    one = one.replace('_SIM_ID_', items[0])
    one = one.replace('_DATE_', items[1])
    one = one.replace('_IP_', items[2])
    one = one.replace('_HOST_', items[3])
    # my @records = split m/\t/,$record;
    # (my $file_name = $records[3]) =~ s/\.txt$//;
    #  my $one = $row_template;
    #  $one =~ s/_SIM_ID_/$records[0]/g;
    #  $one =~ s/_DATE_/$records[1]/;
    #  $one =~ s/_IP_/$records[2]/;
    #  $one =~ s/_HOST_/$records[3]/;

    rows = rows + one
    # }

if len(records) == 0:
    html = ''  # unless @record;
html = html.replace('_SET_', set)
# $html =~ s/_ROWS_/$rows/;
html = html.replace('_ROWS_', rows)
if set == 'ReEDS':
    html = html.replace('_blue', '_tan')
    # $html =~ s/_blue/_tan/g if $set eq 'ReEDS';
if list != 0:
    html = list_html
    # $html = $list_html if $list;

#######################################################################
######################  Serve HTML  ###################################

print(html)

# exit;
