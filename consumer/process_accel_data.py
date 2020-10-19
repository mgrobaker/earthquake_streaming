import boto3, botocore
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import json

#initialize bucket variables
#we will be getting the device metadata (location) from here
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'

#connect to local postgres instance
conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")  

#use pandas to process accel_data stored in postgres
#this transforms accel data into sensor events
def process_accel_data():
    sql = 'select * from accel_data'
    pd1 = pd.read_sql_query(sql, conn)

    #print a few records
    #print(pd1)

    #order by device and time
    pd1 = pd1.sort_values(by=['device_id', 'device_t']).reset_index(drop=True)

    #drop device 15, which has too much noise
    pd2 = pd1[pd1['device_id'] != '015'].reset_index(drop=True)

    #get time between events by lagging the data and taking difference
    pd2['device_t_lag'] = pd2.groupby(['device_id'])['device_t'].shift(1)
    pd2['time_intvl'] = (pd2['device_t'] - pd2['device_t_lag']).round(1)

    #calculate xyz avg accel
    pd2['xyz_avg'] = pd2.loc[:, ['x_avg', 'y_avg', 'z_avg']].mean(axis=1)

    #bucket those time intervals
    #high acceleration readings occuring within several seconds can be grouped into an event
    pd2['time_intvl_bucket'] = np.where(pd2['time_intvl'] < 3, 'same event', 'new event')
    pd2['newevent_flag'] = np.where(pd2['time_intvl'] < 3, 0, 1)

    #assign a sequence to each reading, based on which event it is in
    pd2['event_id'] = pd2['newevent_flag'].cumsum()

    #set values for duration and start time; these will be used to aggregate
    pd2['duration'] = np.where(pd2['time_intvl_bucket'] == 'same event', pd2['time_intvl'], 0)
    pd2['start_time'] = np.where(pd2['time_intvl_bucket'] == 'new event', pd2['device_t'], 0)

    #group from the accel data into sensor events
    #sum to get duration
    sensor_event_times = pd2.loc[:, ['device_id', 'event_id', 'start_time', 'duration']].groupby(['device_id', 'event_id']).sum()
    #average to get mean accelerometer readings
    sensor_event_intensities = pd2.loc[:, ['device_id', 'event_id', 'xyz_avg']].groupby(['device_id', 'event_id']).mean()

    #join the two aggregation results
    sensor_events = sensor_event_times.join(sensor_event_intensities, ['device_id', 'event_id'])

    #only keep events with sufficient duration
    sensor_events = sensor_events[sensor_events['duration'] > 0]

    #convert from epoch time to utc
    sensor_events['start_time_utc'] = pd.to_datetime(sensor_events.loc[:, 'start_time'], unit='s')
    sensor_events['start_date'] = sensor_events.loc[:, 'start_time_utc'].dt.date

    
    #clean index and convert device id to int
    sensor_events.reset_index(inplace=True)
    sensor_events[['device_id']] = sensor_events[['device_id']].astype('int64')
    device_ids_pd[['device_id']] = device_ids_pd[['device_id']].astype('int64')

    #join to get lat and long
    sensor_events = sensor_events.merge(device_ids_pd, on='device_id')

    #write final dataframe to postgres, in a new table
    write_sensor_events(sensor_events)
    
    #take counts, for debugging purposes
    device_ct = pd2.groupby(['device_id']).size().reset_index(name='count')
    intvl_ct = pd2.groupby(['time_intvl']).size().reset_index(name='count').sort_values(['count'])
    bucket_ct = pd2.groupby(['time_intvl_bucket']).size().reset_index(name='count')

    #print statements for debugging
    """
    print(pd2)
    #print(device_ct)
    #print(intvl_ct.iloc[-20:,:])
    print(bucket_ct)
    #print(same_event)
    print(sensor_events)
    """

#writes pandas df to postgres
def write_sensor_events(sensor_events):
    #truncate sensor_events table, to prepare for write
    cur = conn.cursor()
    sql = 'drop table if exists sensor_events;'
    cur.execute(sql)
    cur.close()
    
    #write to postgres
    #this uses sqlalchemy library to write a pd df to postgres directly
    engine = create_engine('postgresql://pulsarcon:a@localhost:5432/postgres')
    sensor_events.to_sql('sensor_events', engine)

    #write to csv
    sensor_events.to_csv('../csvout/sensor_events.csv',index=False,encoding='utf-8')

#download device metadata
def download_device_list():
    file_name = 'devices.jsonl'
    BUCKET_PATH = 'devices/country_code=mx/{}'.format(file_name)

    s3.Bucket(BUCKET_NAME).download_file(
        BUCKET_PATH, '../input_sensor_list/{}'.format(file_name))

#parse device metadata to get ID and location (lat/long)
#Note: similar code to this function exists in producer
#however, producer code is deployed on a different instance
#therefore, it needs to be replicated here and cannot be imported
def parse_device_list():
    global device_ids_pd

    #list that will be used to generate the df
    device_id_list = []

    #open file
    file_name = 'devices.jsonl'        
    file = open('../input_sensor_list/{}'.format(file_name),'r').read()
    file_lines = file.splitlines()

    #parse file
    for line in file_lines:
        line_cols = json.loads(line)
        #only add if the device has an 'is_current_row' flag set to True
        if line_cols['is_current_row']:
            #print('device_id: {}'.format(line_cols['device_id']))
            device_id_list.append([line_cols['device_id'],
                                   line_cols['latitude'],
                                   line_cols['longitude']])
            #print(line_cols['latitude'])

    #convert list to pd df
    device_ids_pd = pd.DataFrame(device_id_list, columns=['device_id', 'latitude', 'longitude'])
    #print(device_ids_pd)
    
def main():
    #get device metadata
    download_device_list()
    parse_device_list()

    #process
    process_accel_data()

    #close postgres conn
    conn.close()
    
if __name__ == "__main__": 
    main()         

