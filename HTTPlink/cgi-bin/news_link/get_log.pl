#!/usr/bin/perl -w

use strict;
use warnings;
use CGI qw/header param/;
use File::Path qw/rmtree/;
use IO::Seekable;


#######################################################################
##################   Read Arguments/Parameters   ######################

my $set		= param('set')     || 'TP2M';
my $rm_name = param('remove')  ||  0;
my $newname	= param('newname') ||  0;
my $move	= param('move')    ||  0;
my $list	= param('list')    ||  0;

#######################################################################

my $htdocs	= '/var/www/news_link';
# my $htdocs	= '/net/nfs/merrimack/web/earthatlas/htdocs';
my $run_dir	= "/news_transfer/$set/";
my $log_file	= $htdocs.$run_dir.'status.txt';

#######################################################################
##################   Remove/Rename Record   ###########################

if ($rm_name) {
		### Rename/Remove directories
  if ($newname) {
    rename $htdocs.$run_dir.$rm_name, $htdocs.$run_dir.$newname;
  }
  else {
    rmtree $htdocs.$run_dir.$rm_name;
  }
		### Update Log file
  my $log = '';
  if (open (FILE,"<$log_file")) {
    while (my $line = <FILE>)
    {
      if ($newname) {
	$line =~ s/^$rm_name\t/$newname\t/;
	$log .= $line;
      }
      else {
	$log .= $line if $line !~ m/^$rm_name\t/;
      }
    }
    close FILE;

    open (FILE,">$log_file") or die "Couldn't open $log_file, $!";
      print FILE $log;
    close FILE;
  }
}

#######################################################################
##################   Move a recond Up/Down   ##########################

if ($move) {
  open (FILE,"<$log_file") or die "Couldn't open $log_file, $!";
    my @lines = <FILE>;
  close FILE;

  my $ind = (grep {$lines[$_]=~m/^$newname\t/} 0..$#lines)[0];
  if (defined($ind) && (($move == -1 && $ind != 0) || ($move == 1 && $ind != $#lines))) {
    splice(@lines,$ind+$move,0,splice(@lines,$ind,1));

    open (FILE,">$log_file") or die "Couldn't open $log_file, $!";
	print FILE @lines;
    close FILE;
  }
}

#######################################################################
##################   Move a recond Up/Down   ##########################

my  $list_html = '';
if ($list) {
  my $list_dir	= $htdocs.$run_dir.$list;
  opendir (DIR, $list_dir) || die "Can't opendir $list_dir: $!";
    my @files	= sort grep(m/^\w/,readdir(DIR));
    foreach my $file (@files) {
      $list_html .= "<a href=\"$run_dir$list/$file\" style=\"font-weight: bold; color: blue; text-decoration: none\">$file</a><br />\n";
    }
  closedir DIR;
}

#######################################################################
######################  Read Data  ####################################

my @record = getLastRecords($log_file,200) unless $list;

#######################################################################
######################   Make HTML  ###################################

my $html = <<EOF;
<table class="table_top">
  <tr>
    <th class="cell_header_blue" colspan="6"><span style="font-size:larger;">$set</span>
	<a href="" class="refresh-log">(Refresh Now:</a> Auto-refresh interval is 1 hour)</th>
  </tr>
  <tr>
    <th style="width: 35%" class="cell_header_blue">Simulation ID</th>
    <th style="width:  5%" class="cell_header_blue">Move</th>
    <th style="width: 10%" class="cell_header_blue">Action</th>
    <th style="width: 15%" class="cell_header_blue">Date</th>
    <th style="width: 15%" class="cell_header_blue">IP</th>
    <th style="width: 20%" class="cell_header_blue">Host</th>
  </tr>
_ROWS_
</table>
EOF

my $row_template = <<EOF;
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
EOF

my $rows = '';
foreach my $record (@record)
{
  my @records = split m/\t/,$record;
 (my $file_name = $records[3]) =~ s/\.txt$//;
  my $one = $row_template;
  $one =~ s/_SIM_ID_/$records[0]/g;
  $one =~ s/_DATE_/$records[1]/;
  $one =~ s/_IP_/$records[2]/;
  $one =~ s/_HOST_/$records[3]/;

  $rows .= $one;
}

$html = '' unless @record;
$html =~ s/_ROWS_/$rows/;
$html =~ s/_blue/_tan/g if $set eq 'ReEDS';
$html = $list_html if $list;


#######################################################################
######################  Serve HTML  ###################################

print header();
print $html;

exit;

#######################################################################
######################  Functions  ####################################

sub getLastRecords		### Read Last records in a log file

{
  my ($file,$n_records) = @_;
  my @records = ();

  if (open (FILE, "<$file")) {
	@records = <FILE>;
	close FILE;
	map chomp,@records;
  }

  $n_records = scalar(@records) if $n_records > scalar(@records);

  return reverse(@records[-$n_records..-1]);
}

#########################################################################
