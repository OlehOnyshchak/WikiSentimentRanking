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

df = pd.DataFrame(columns=['name','neg', 'neu', 'pos', 'compound'])

def insertDataFromJSON(filepath):
    global df
    
    with open(filepath) as json_file:
        json_data = json.load(json_file)

        df = df.append({
            'name': json_data['name'], 
            'neg': json_data['emotions']['neg'],
            'neu': json_data['emotions']['neu'],
            'pos': json_data['emotions']['pos'],
            'compound': json_data['emotions']['compound']
        }, ignore_index=True)
        

### observe new files
def on_file_created(filepath):
    global df
    
    if(not filepath.endswith(".json")):
        print(df)
        return
    
    insertDataFromJSON(filepath)
    
    df = df.sort_values(by=['compound'], ascending=False).reset_index(drop=True)

    mean_text = 'Mean: {}'.format(round(df["compound"].mean(),3))
    display(mean_text)
    display(df)

def observe(path, files_count, pb):

    class EventHandler(FileSystemEventHandler):
        def on_created(self, event):
            pb.value += 1
            pb.description = 'Iter {}/{}'.format(pb.value, files_count)
            clear_output(wait=True)
            display(pb)
            on_file_created(event.src_path)

    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    
def process_output_files(path, files_count):
    global df
    
    pb = IntProgress(description='Iter 0/{}'.format(files_count), min=0, max=files_count)
    display(pb)
    
    df.drop(df.index, inplace=True)

    ### read existing files
    for filename in [f for f in listdir(path) if isfile(join(path, f))]:

        if(not filename.endswith(".json")):
            continue

        insertDataFromJSON(join(path, filename))
        
        pb.value += 1
        pb.description = 'Iter {}/{}'.format(pb.value, files_count)

    df = df.sort_values(by=['compound'], ascending=False).reset_index(drop=True)
    mean_text = 'Mean: {}'.format(round(df["compound"].mean(),3))

    display(mean_text)
    display(df)

    ### observe

    observe(path, files_count, pb)