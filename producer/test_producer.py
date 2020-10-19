import pulsar
import time

#pulsar producer
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
#broker2_url = 'pulsar://ec2-3-19-119-137.us-east-2.compute.amazonaws.com:6650' 

client = pulsar.Client(broker1_url)
producer = client.create_producer(topic='sensors', batching_enabled=True)

def callback(res, msg):
    return

#test speed by sending short messages
t0 = time.time()
num_msgs = 5000
for i in range (num_msgs):
#    producer.send_async(('Hello-%d' %i).encode('utf-8'), callback)
    producer.send(('Hello-%d' %i).encode('utf-8'))

#calculate send time and print results
t1 = time.time()
send_time = t1-t0
print('NUM MSGS SENT: {}'.format(num_msgs))
print('SEND TIME: {}'.format(send_time))
