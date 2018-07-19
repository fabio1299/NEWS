#!/usr/bin/python
#Cron job to 

import cgi
import os
import sys


def check_status_reeds():

   with open('/var/www/html/status_reeds.txt') as f:
       content = f.readlines()
       content = [x.strip('\n') for x in content]
       content = ''.join(content)

   return(content)



# Required header that tells the browser how to render the HTML.
print "Content-Type: text/html\n\n"



if(check_status_reeds() == 'wait'):
       print "Reeds Wait"
       quit()

if(check_status_reeds() == 'exit'):
       print "Cron got exit response"
       quit()

if(check_status_reeds() == 'ready'):
     #Run TP@M and write out a gdx file
     print "Reeds Ready"

