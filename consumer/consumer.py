import pulsar
import json
import psycopg2
from psycopg2 import extras as psy_ext

conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'
    
#make an insertion list like ((1,2,3), (4,5,6), (7,8,9))
def make_insertion_list(x, y, z):
    num_rows = max(len(x), len(y), len(z))
    insertion_list = []
    
    for i in range(0, num_rows):
        try:
            xval = x[i]
        except IndexError:
            xval = 0

        try:
            yval = y[i]
        except IndexError:
            yval = 0

        try:
            zval = z[i]
        except IndexError:
            zval = 0
        
        #add to list of rows
        new_row = [xval, yval, zval]
        insertion_list.append(new_row)
        
    return insertion_list
            
def insert_list(insertion_list):
        print("insertion_list:")
        print(insertion_list)

        cur = conn.cursor()
        #insert all the values
        psy_ext.execute_values(cur, "INSERT INTO samp_accel_values VALUES %s", insertion_list)
        conn.commit()
        cur.close()

def listen():
    
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

def main():
    x = [1, 2, 3]
    y = [4.2, 5.3, 6.4]
    z = [8, 9]

    insertion_list = make_insertion_list(x, y, z)
    insert_list(insertion_list)

    #initialize consumer
    #client = pulsar.Client(broker1_url)
    #consumer = client.subscribe('sensors', 'sensor-subscription')
    
    conn.close()

if __name__ == "__main__":
    main()
        
