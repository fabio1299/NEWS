#!/usr/bin/python
#Cron job to 

import cgi
import os
import sys


def check_status_tp2m():

   with open('/var/www/news_link/status_tp2m.txt') as f:
       content = f.readlines()
       content = [x.strip('\n') for x in content]
       content = ''.join(content)

   return(content)



# Required header that tells the browser how to render the HTML.
print "Content-Type: text/html\n\n"



if(check_status_tp2m() == 'wait'):
       print "TP2M Wait"
       quit()

if(check_status_tp2m() == 'exit'):
       print "Cron got exit response"
       quit()

if(check_status_tp2m() == 'ready'):
     #Run TP@M and write out a gdx file
     print "TP2M Ready"

