import pulsar
import time
from openeew.data import aws as eew

#pulsar client
#broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
#client = pulsar.Client(broker1_url)
#producer = client.create_producer(topic='sensors')

#set start time
start_time = '2020-09-15 10:00:00'
end_time = '2020-09-15 10:10:00'
device_ids = ['010']

#use openeew library to connect to bucket
eew_aws_client = eew.AwsDataClient('mx')
eew_data = eew_aws_client.get_filtered_records(start_time, end_time)
#start_date = eew.DateTimeKeyBuilder(year='2020',month='09',day='15',hour='10',minute='00')
        
#client.close()

