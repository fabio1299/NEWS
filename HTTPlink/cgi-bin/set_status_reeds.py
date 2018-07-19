#!/usr/bin/python

import cgi
import os
import sys


fs = cgi.FieldStorage()

print "Content-type: text/plain\n"

#for key in fs.keys():
#    print "%s = %s" % (key, fs[key].value)

status = fs['status'].value


url = '/var/www/html/status_reeds.txt'
f = open(url,"w")
f.write(status)
f.close()

