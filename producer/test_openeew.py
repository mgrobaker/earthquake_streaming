import pulsar
import time
from openeew.data import aws as eew

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'

#client = pulsar.Client(broker1_url)
#producer = client.create_producer(topic='sensors')

eew_aws_client = eew.AwsDataClient('mx')
start_time = '2020-09-15 10:00:00'
end_time = '2020-09-15 10:10:00'
device_ids = ['010']

eew_data = eew_aws_client.get_filtered_records(start_time, end_time)
#start_date = eew.DateTimeKeyBuilder(year='2020',month='09',day='15',hour='10',minute='00')

#records = 

for i in range (10):
    print(i)
    #producer.send(('Hello-%d' %i).encode('utf-8'))
    #time.sleep(100)
        
#client.close()

