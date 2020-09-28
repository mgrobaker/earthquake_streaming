import boto3
import botocore

BUCKET_NAME = 'grillo-openeew'
KEY = '/records/country_code=mx/device_id=010/year=2020/month=09/day=17/hour=02/00.jsonl'

#s3 = boto3.client('s3')
s3 = boto3.resource('s3')

#response = s3.list_buckets()

# Output the bucket names
"""
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')
"""

try:
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'test.jsonl')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

