import json
import statistics as stat

import pulsar

import psycopg2
from psycopg2 import extras as psy_ext

conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'

def make_insertion_list(country_code, device_id, device_time, x, y, z):
    abs_x = list(map(abs, x))
    abs_y = list(map(abs, y))
    abs_z = list(map(abs, z))    

    max_x = max(abs_x)
    max_y = max(abs_y)
    max_z = max(abs_z)

    mean_x = round(stat.mean(abs_x),2)
    mean_y = round(stat.mean(abs_y),2)
    mean_z = round(stat.mean(abs_z),2)

    insertion_list = []    
    x_threshold = 0.05
    y_threshold = x_threshold
    
    if ( (mean_x > x_threshold) and (mean_y > y_threshold)):
        #create new row for insertion
        """
        print('creating insertion list')
        print(country_code)
        print(device_id)
        print(device_time)
        print(max_x)
        print(mean_x)
        """
        
        new_row = [country_code, device_id, device_time,
                          max_x, max_y, max_z,
                          mean_x, mean_y, mean_z]
        insertion_list.append(new_row)
        
    return insertion_list

#make an insertion list like ((1,2,3), (4,5,6), (7,8,9))
def make_granular_insertion_list(x, y, z):
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
        #print("insertion_list:")
        #print(insertion_list)

        cur = conn.cursor()
        #insert all the values
        psy_ext.execute_values(cur, "INSERT INTO accel_data VALUES %s", insertion_list)

        #result_msg = cur.fetchall()
        #for line in result_msg:
        #    print(line)
        
        conn.commit()
        cur.close()

def insert_granular_list(insertion_list):
        print("insertion_list:")
        print(insertion_list)

        cur = conn.cursor()
        #insert all the values
        psy_ext.execute_values(cur, "INSERT INTO samp_accel_values VALUES %s", insertion_list)
        conn.commit()
        cur.close()

def listen(consumer):
    while True:
        #print('waiting for msgs...')
        msg = consumer.receive()
        try:
            msg_data = msg.data()
            print("Received message id={}".format(msg.message_id()))
            #print("Received message '{}' id='{}'".format(msg_data, msg.message_id()))
            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)

            msg_json_str = msg_data.decode('utf8')
            msg_cols = json.loads(msg_json_str)

            insertion_list = make_insertion_list(msg_cols['country_code'],
                msg_cols['device_id'], msg_cols['device_t'],
                msg_cols['x'], msg_cols['y'], msg_cols['z'])

            #if (len(insertion_list) > 0):
                #insert_list(insertion_list)
            
        except:
            # Message failed to be processed
            consumer.negative_acknowledge(msg)

    client.close()

def test_insert():
    x = [1, 2, 3]
    y = [4.2, 5.3, 6.4]
    z = [8, 9]

    insertion_list = make_granular_insertion_list(x, y, z)
    insert_granular_list(insertion_list)
    
def main():
    #test_insert()

    #initialize consumer
    client = pulsar.Client(broker1_url)
    consumer = client.subscribe('sensors', 'sensor-subscription')
    listen(consumer)
    
    conn.close()

if __name__ == "__main__":
    main()
