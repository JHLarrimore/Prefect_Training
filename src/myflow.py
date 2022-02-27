'''
Initial prefect project from the following tutorial:
https://www.youtube.com/watch?v=0IcN117E4Xo
'''
import os
from os import walk
import csv
import datetime

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

@task(max_retries=5, retry_delay=datetime.timedelta(seconds=5))
def extract(path):
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    print(data)
    return data

@task
def transform(data):
    tdata = [i+1 for i in data]
    return tdata

@task
def load(data, path):
    with open(path, "w" ) as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    return 

def build_flow(schedule=None):
    with Flow("my_etl", schedule=schedule) as flow:
        extract_path = Parameter(name="extract_path", required=True)
        load_path = Parameter(name="load_path", required=True)
        data = extract(extract_path)
        tdata = transform(data)
        load(tdata, load_path)
    return flow

schedule = IntervalSchedule(
    start_date=datetime.datetime.now() + datetime.timedelta(seconds=10),
    interval=datetime.timedelta(seconds=5)
)

flow = build_flow(schedule)
dir_path = os.path.dirname(os.path.realpath(__file__))
extract_path = r"/home/jake/Desktop/source/prefect/src/values.csv"
load_path = r"/home/jake/Desktop/source/prefect/src/tvalues.csv"
flow.run(parameters={
    "extract_path": extract_path, 
    "load_path": load_path
    })
    