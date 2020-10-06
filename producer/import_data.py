import boto3, botocore
import pulsar
from pulsar.schema import *
import time
import json

BUCKET_NAME = 'grillo-openeew'

PREFIX = 'records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour=02/'
OBJ_NAME = '00.jsonl'
FULL_PATH = PREFIX + OBJ_NAME

s3 = boto3.resource('s3')
#response = s3.list_buckets()
#response = s3.Bucket(BUCKET_NAME).objects.all()
response = s3.Bucket(BUCKET_NAME).objects.filter(Prefix=PREFIX)

# Output the bucket names
print('Existing buckets:')
for obj in response:
    print(obj)

#get one of the objects
s3obj = s3.Object(BUCKET_NAME, FULL_PATH).get()['Body'].read()
sensor_rdgs = s3obj.splitlines()

#initialize Pulsar producer
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
client = pulsar.Client(broker1_url)
#producer = client.create_producer(topic='sensors', schema=JSONSchema())
producer = client.create_producer(topic='sensors')

#send messages
i = 0
for line in sensor_rdgs:
    producer.send(line)
    #producer.send(json.dumps(line.decode('utf-8')))
    #print(json.dumps(line.decode('utf-8')))
    i += 1
    if i > 10: break
        
client.close()
