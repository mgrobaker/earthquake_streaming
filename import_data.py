import boto3
import botocore

BUCKET_NAME = 'grillo-openeew'

PREFIX = 'records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour=02/'
OBJ_NAME = '00.jsonl'
FULL_PATH = PREFIX + OBJ_NAME

#s3 = boto3.client('s3')
s3 = boto3.resource('s3')

#response = s3.list_buckets()
#response = s3.Bucket(BUCKET_NAME).objects.all()
response = s3.Bucket(BUCKET_NAME).objects.filter(Prefix=PREFIX)

# Output the bucket names
print('Existing buckets:')
for obj in response:
    print(obj)

#get one of the objects
s3.Bucket(BUCKET_NAME).download_file(FULL_PATH, OBJ_NAME)

#1TB. do i download it first, or just send as pulsar msgs directly?
#not sure i can download it fast enough..

"""
try:
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'test.jsonl')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
"""
