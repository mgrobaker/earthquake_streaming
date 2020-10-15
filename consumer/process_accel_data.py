import boto3, botocore
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
import datetime as dt
import json

#initialize bucket variables
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'

conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")  

#read in the data into pandas
#def read_data():
def process_accel_data():
    sql = 'select * from accel_data'
    pd1 = pd.read_sql_query(sql, conn)

    #print a few records
    print(pd1)

    #order the pd data by device and time
    pd1 = pd1.sort_values(by=['device_id', 'device_t']).reset_index(drop=True)

    #drop device 15, which has too much noise
    pd2 = pd1[pd1['device_id'] != '015'].reset_index(drop=True)

    #get time between events
    pd2['device_t_lag'] = pd2.groupby(['device_id'])['device_t'].shift(1)
    pd2['time_intvl'] = (pd2['device_t'] - pd2['device_t_lag']).round(1)

    #calculate xyz avg accel
    pd2['xyz_avg'] = pd2.loc[:, ['x_avg', 'y_avg', 'z_avg']].mean(axis=1)

    #bucket those time intervals
    pd2['time_intvl_bucket'] = np.where(pd2['time_intvl'] < 3, 'same event', 'new event')
    pd2['newevent_flag'] = np.where(pd2['time_intvl'] < 3, 0, 1)
    pd2['event_id'] = pd2['newevent_flag'].cumsum()

    #set values for duration and start time; these will be used to aggregate
    pd2['duration'] = np.where(pd2['time_intvl_bucket'] == 'same event', pd2['time_intvl'], 0)
    pd2['start_time'] = np.where(pd2['time_intvl_bucket'] == 'new event', pd2['device_t'], 0)

    #group up
    sensor_event_times = pd2.loc[:, ['device_id', 'event_id', 'start_time', 'duration']].groupby(['device_id', 'event_id']).sum()
    sensor_event_intensities = pd2.loc[:, ['device_id', 'event_id', 'xyz_avg']].groupby(['device_id', 'event_id']).mean()

    #join to get with time and intensity
    sensor_events = sensor_event_times.join(sensor_event_intensities, ['device_id', 'event_id'])

    #drop short events
    sensor_events = sensor_events[sensor_events['duration'] > 0]

    #calculate utc
    sensor_events['start_time_utc'] = pd.to_datetime(sensor_events.loc[:, 'start_time'], unit='s')

    #join to get lat and long in here
    sensor_events.reset_index(inplace=True)
    device_ids_pd
    sensor_events[['device_id']] = sensor_events[['device_id']].astype('int64')
    device_ids_pd[['device_id']] = device_ids_pd[['device_id']].astype('int64')
    
    #print(sensor_events.dtypes)
    #print(device_ids_pd.dtypes)
    sensor_events = sensor_events.merge(device_ids_pd, on='device_id')
    
    #take counts
    device_ct = pd2.groupby(['device_id']).size().reset_index(name='count')
    intvl_ct = pd2.groupby(['time_intvl']).size().reset_index(name='count').sort_values(['count'])
    bucket_ct = pd2.groupby(['time_intvl_bucket']).size().reset_index(name='count')

    #print results
    #print(pd1)
    print(pd2)
    #print(device_ct)
    #print(intvl_ct.iloc[-20:,:])
    print(bucket_ct)
    #print(same_event)
    print(sensor_events)

def write_sensor_events(sensor_events):
    #write to postgres

    #write to csv
    sensor_events.to_csv('../csvout/sensor_events.csv',index=False,encoding='utf-8')
    
    
def download_device_list():
    file_name = 'devices.jsonl'
    BUCKET_PATH = 'devices/country_code=mx/{}'.format(file_name)

    s3.Bucket(BUCKET_NAME).download_file(
        BUCKET_PATH, '../input_sensor_list/{}'.format(file_name))

#Note: similar code to this function exists in producer
#however, producer code is deployed on a different instance
#therefore, it needs to be replicated here and cannot be imported
def parse_device_list():
    #make list for each col, go thru lines, and append to it
    global device_ids_pd
    device_id_list = []

    #open file
    file_name = 'devices.jsonl'        
    file = open('../input_sensor_list/{}'.format(file_name),'r').read()
    file_lines = file.splitlines()
    
    for line in file_lines:
        line_cols = json.loads(line)
        if line_cols['is_current_row']:
            #print('device_id: {}'.format(line_cols['device_id']))
            device_id_list.append([line_cols['device_id'],
                                   line_cols['latitude'],
                                   line_cols['longitude']])
            #print(line_cols['latitude'])

    device_ids_pd = pd.DataFrame(device_id_list, columns=['device_id', 'latitude', 'longitude'])
    #print(device_ids_pd)
    
def main():
    download_device_list()
    parse_device_list()

    process_accel_data()
    
    conn.close()
    
if __name__ == "__main__": 
    main()         

