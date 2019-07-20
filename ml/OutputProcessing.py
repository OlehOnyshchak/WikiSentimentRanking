import time

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler

from IPython.display import display
from IPython.display import clear_output
from ipywidgets import IntProgress

import json

import pandas as pd

from os import listdir
from os.path import isfile, join

pb = IntProgress(min=0, max=0) # to be created later
df = pd.DataFrame(columns=['title','url', 'sentiment'])

new_data_arrived = False
log_last_updated_millis = 0
log_update_interval_millis = 3000

def updateLogIfNeeded():
    
    global log_last_updated_millis
    global log_update_interval_millis
    global new_data_arrived
    global pd
    global df
    
    millis = int(round(time.time() * 1000))
    
    if not new_data_arrived:
        return
    
    if millis - log_last_updated_millis < log_update_interval_millis:
        return
    
    new_data_arrived = False
    log_last_updated_millis = millis
    
    clear_output(wait=True)
    display(pb)
    
    mean_text = 'Mean: {}'.format(round(df["sentiment"].mean(),3))
    display(mean_text)
    display(df)
    
def insertDataFromJSON(filepath):
    global df
    global new_data_arrived
    
    with open(filepath) as json_file:
        json_data = json.load(json_file)

        df = df.append({
            'title': json_data['title'], 
            'url': json_data['url'],
            'sentiment': json_data['sentiment']
        }, ignore_index=True)
        
        new_data_arrived = True
        
def on_file_created(filepath):
    global df
    global new_data_arrived
    
    insertDataFromJSON(filepath)
    
    df = df.sort_values(by=['sentiment'], ascending=False).reset_index(drop=True)
    
    new_data_arrived = True

def observe(path, files_count, pb):

    class EventHandler(FileSystemEventHandler):
        
        def on_moved(self, event):
            
            filepath = event.dest_path
            
            if "_temporary" in filepath:
                return
            if(not filepath.endswith(".json")):
                return
            
            pb.value += 1
            pb.description = 'Iter {}/{}'.format(pb.value, pb.max)
            
            on_file_created(filepath)
    
    observer = Observer()
    observer.schedule(EventHandler(), path, recursive=True)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
            
            updateLogIfNeeded()
            
            if pb.value >= pb.max:
                observer.stop()
                return
            
    except KeyboardInterrupt:
        observer.stop()
        
    observer.join()
    
def process_output_files(path, files_count):
    global df
    global pb
    
    df = pd.DataFrame(columns=['title','url', 'sentiment'])

    pb = IntProgress(description='Iter 0/{}'.format(files_count), min=0, max=files_count)
    display(pb)

    observe(path, files_count, pb)