#!/usr/bin/python

import cgi, os
import cgitb; cgitb.enable()

#Just allows binary file to be uploaded to the system


form_data = cgi.FieldStorage()
file_data = form_data['file']

if file_data.filename:
   fn = os.path.basename(file_data.filename.replace("\\", "/" ))
   open('/var/www/html/data_exchange/'+fn, 'wb').write(file_data.file.read())
   fp.write(file_data)
   fp.close()
