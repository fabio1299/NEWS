import time
import sys
import logging
import datetime as dt
# from django.shortcuts import render
#
# from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
# date=dt.now()

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
# changes = ''
class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.xml", "*.lxml", "*.xlsx", "*.txt","*.py"]

    def process(self, event):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        # the file will be processed there
        print(event.src_path, event.event_type) # print now only for debug
        # print(date)
 #        changes = ''
 #        changes +=''' Path : ''' +str ( event.src_path )+'''nbsp& ''' +str(event.event_type)+'''nbsp& ''' +str(date)+'''
 # '''
 #
 #        return render(
 #
 #        )


    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)



if __name__ == '__main__':
    args = sys.argv[1:]
    observer = Observer()
    observer.schedule(MyHandler(), path=args[0] if args else '.')
    observer.start()

    try:
        while True:
            time.sleep(1)
            # print('checking')
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

