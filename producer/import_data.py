import boto3, botocore
import pulsar
from pulsar.schema import *
import time
import json

#initialize pulsar producer
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
client = pulsar.Client(broker1_url)
producer = client.create_producer(topic='sensors')

#initialize bucket variables
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'
OBJ_NAME = '00.jsonl'

YEAR = '2020'

device_id = '010'

#make lists of months, days, and hours, to iterate through
months_list = list(range(1,13))
#days_list = range(1,31)
days_list = list(range(1,4))
hours_list = list(range(0,24))

#convert to 0-padded strings, as required by openeew
for i in range(0, len(months_list)):
    months_list[i] = '{0:02}'.format(months_list[i])
for i in range(0, len(days_list)):
    days_list[i] = '{0:02}'.format(days_list[i])
for i in range(0, len(hours_list)):
    hours_list[i] = '{0:02}'.format(hours_list[i])

files_sent = 0
#add timing logic

#also, i have to iterate through all the JSON files. not just 00

#PREFIX = 'records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour=02/'
#FULL_PATH = PREFIX + OBJ_NAME

#iterate through the objects in the bucket
for day in days_list:
    #print('day: {}'.format(day))
    for hour in hours_list:
        BUCKET_PATH = 'records/country_code=mx/device_id={}/year={}/month={}/day={}/hour={}/{}'.format(device_id, YEAR, months_list[8], day, hour, OBJ_NAME)
        #BUCKET_PATH = 'records/country_code=mx/device_id=010/year=2020/month=09/day={}/hour={}/{}'.format(day, hour, OBJ_NAME)
        #print('HOUR: {}'.format(hour))
        #print('{}'.format(BUCKET_PATH))

        #s3obj = s3.Object(BUCKET_NAME, BUCKET_PATH).get()['Body'].read()
        
        try:
            #s3obj = s3.Object(BUCKET_NAME, BUCKET_PATH).get()['Body'].read()
            #s3obj = s3.Object(BUCKET_NAME, FULL_PATH).get()['Body'].read()

            #download file
            download_file_name = '../input_data/device{}_yr{}_mon{}_day{}_hr{}_{}'.format(device_id, YEAR, months_list[8], day, hour, OBJ_NAME)            
            s3.Bucket(BUCKET_NAME).download_file(BUCKET_PATH, download_file_name)
            
            sensor_rdgs = s3obj.splitlines()
            print('sent: {}'.format(BUCKET_PATH))
            files_sent += 1

            #send messages
            i = 0
            #for line in sensor_rdgs:
                #print(line)
                ##producer.send(line)

                #i += 1
                #if i > 3: break
        except:
            #if no file is found at this location, skip
            #this prevents producer from breaking due to invalid path
            continue

print('NUM FILES SENT: {}'.format(files_sent))
client.close()
