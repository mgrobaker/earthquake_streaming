import pulsar

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
client = pulsar.Client(broker1_url)
producer = client.create_producer(topic='sensors')

for i in range (10):
    producer.send(('Hello-%d' %i).encode('utf-8'))
