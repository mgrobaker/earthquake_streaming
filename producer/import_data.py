import boto3, botocore
import pulsar
from pulsar.schema import *
import time
import json

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
client = pulsar.Client(broker1_url)

s3 = boto3.resource('s3')
#response = s3.Bucket(BUCKET_NAME).objects.filter(Prefix=PREFIX)

BUCKET_NAME = 'grillo-openeew'
PREFIX = 'records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour=02/'
OBJ_NAME = '01.jsonl'
FULL_PATH = PREFIX + OBJ_NAME

#initialize Pulsar producer
###producer = client.create_producer(topic='sensors', schema=JSONSchema())
#producer = client.create_producer(topic='sensors')


months_list = range(1,13)
days_list = range(1,31)
hours_list = range(0,24)

#reformat
for hour in hours_list: hour = '{0:02}'.format(hour)

#get one of the objects

#for day in days_list:
for hour in hours_list:
    hour_s = '{0:02}'.format(hour)
    print('HOUR: {}'.format(hour_s))
    PREFIX = 'records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour={}/'.format(hour_s)

    try:
        s3obj = s3.Object(BUCKET_NAME, FULL_PATH).get()['Body'].read()
        sensor_rdgs = s3obj.splitlines()

        #send messages
        i = 0
        for line in sensor_rdgs:
            print(line)
            #producer.send(line)
        
            i += 1
            if i > 3: break
    except:
        #if no file is found at this location, skip
        #this prevents producer from breaking due to invalid path
        continue

    
client.close()
