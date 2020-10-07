import pulsar
import json
import psycopg2
from psycopg2 import extras as psy_ext

#initialize consumer
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
#client = pulsar.Client(broker1_url)
#consumer = client.subscribe('sensors', 'sensor-subscription')

#write to postgres
x = [1, 2, 3]
y = [4.2, 5.3, 6.4]
z = [8, 9, 10]
num_rows = len(x)
insert_str = ['']

###generate insertion string (make a function)
#create the string
#join it to the list
for i in range(0, num_rows):
    if i < num_rows - 1:
        new_row = "({}, {}, {}), ".format(x[i], y[i], z[i])
    else:
        new_row = "({}, {}, {})".format(x[i], y[i], z[i])        

    insert_str.append(new_row)

final_str = ''.join(insert_str)    
print("insert_str: " + final_str)
    
###
conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")

cur = conn.cursor()
#for i in range(1,num_rows):

psy_ext.execute_values(cur, "INSERT INTO samp_accel_values (x, y, z) VALUES %s",
               [(x[2], 2, 3), (4, 5, 6), (7, 8, 9)])

conn.commit()
cur.close()
conn.close()

"""
while True:
    msg = consumer.receive()
    try:
        msg_data = msg.data()
        print("Received message '{}' id='{}'".format(msg_data, msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
        print('attempting parsing...')
        data_str = msg_data.decode('utf8')
        data_dict = json.loads(data_str)
        #data_dict_str = json.dumps(data_dict)
        print(data_dict['x'])
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
"""
