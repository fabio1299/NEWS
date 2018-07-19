#!/usr/bin/python
# -*- coding: utf-8 -*-

import news_transfer_config as cfg
import cgi, os, sys
# import errno added by Fabio Oct. 17, 2017 to allow multiple nested folders
import errno
import cgitb; cgitb.enable()

# Function added by Fabio Oct. 17, 2017 to allow multiple nested folders
def mkdir_p(path):
  try:
     os.makedirs(path)
  except OSError as exc:  # Python >2.5
     if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
     else:
        raise

#Just allows binary file to be uploaded to the system

form_data = cgi.FieldStorage()
file_data = form_data['file']
if 'path' in form_data:
  dir_path = form_data['path'].value
else:
  dir_path = 'data_exchange'

dir_path = cfg.server_htdocs + dir_path.replace("\\","/")  # '/var/www/news_link/news_transfer/' + dir_path.replace("\\","/")
# dir_path = '/web/earthatlas/htdocs/news_transfer/' + dir_path

try:
  os.stat(dir_path)
except:
# Changed by Fabio Oct. 17, 2017 to allow multiple nested folders
#  os.mkdir(dir_path)
  mkdir_p(dir_path)


if file_data.filename:
  fn = os.path.basename(file_data.filename.replace("\\", "/" ))
  fp = open(os.path.join(dir_path,fn.lower()), 'wb')
  fp.write(file_data.file.read())
  fp.close()
print 'Content-type: text/html\n'

