import json
import statistics as stat

import pulsar

import psycopg2
from psycopg2 import extras as psy_ext

#connect to local postgres db
conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")

#pulsar broker from which we will consume messages
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'

#transform message data (passed in the arguments), to prepare for postgres load
#each message is one second of data. ~35 x/y/z accelerometer readings are taken per second
#x, y, and z are lists of those readings, along each 3D axis
def make_insertion_list(country_code, device_id, device_time, x, y, z):
    #capture the largest x/y/z values
    abs_x = list(map(abs, x))
    abs_y = list(map(abs, y))
    abs_z = list(map(abs, z))    

    max_x = max(abs_x)
    max_y = max(abs_y)
    max_z = max(abs_z)

    #calculate mean x/y/z values
    mean_x = round(stat.mean(abs_x),2)
    mean_y = round(stat.mean(abs_y),2)
    mean_z = round(stat.mean(abs_z),2)

    #make list to store the new record we want to insert to postgres
    #we currently make just 1 record, but are using a list in case we want to add many later on
    insertion_list = []

    #establish an arbitrary cutoff for as a simple detection heuristic
    #(data scientists could apply more sophisticated techniques here)
    #if the mean x/y/z readings are higher than cutoff, then we want to store this data
    #it is a possible earthquake and could be interesting to study later
    x_threshold = 0.05
    y_threshold = x_threshold
    
    if ( (mean_x > x_threshold) and (mean_y > y_threshold)):
        #print record, for debugging purposes
        """
        print('creating insertion list')
        print(country_code)
        print(device_id)
        print(device_time)
        print(max_x)
        print(mean_x)
        """
        
        #create new row for insertion
        #we are only going to store the max and mean values, not every sub-second reading
        #(depending on business requirements we could of course store the granular sub-second data as well)
        new_row = [country_code, device_id, device_time,
                          max_x, max_y, max_z,
                          mean_x, mean_y, mean_z]
        insertion_list.append(new_row)
        
    return insertion_list

#function to test out data format required for postgres insertion
#make an insertion list like ((1,2,3), (4,5,6), (7,8,9))
def make_granular_insertion_list(x, y, z):
    num_rows = max(len(x), len(y), len(z))
    insertion_list = []

    #the try-except logic handles for case when one list is shorter than the others
    #this could be useful if we expect some accelerometer readings to be truncated
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

#insert into accel_data table in postgres
def insert_list(insertion_list):
        #print("insertion_list:")
        #print(insertion_list)

        #insert all the values
        cur = conn.cursor()
        psy_ext.execute_values(cur, "INSERT INTO accel_data VALUES %s", insertion_list)
        
        conn.commit()
        cur.close()

#insert into samp_accel_data table in postgres. this table is used for testing
def insert_granular_list(insertion_list):
        print("insertion_list:")
        print(insertion_list)

        #insert all the values
        cur = conn.cursor()
        psy_ext.execute_values(cur, "INSERT INTO samp_accel_values VALUES %s", insertion_list)

        conn.commit()
        cur.close()

#consume messages from pulsar broker
#parse and insert earthquake data
def listen(consumer):
    msgs_recvd = 0
    while True:
        #print('waiting for msgs...')
        msg = consumer.receive()
        try:
            msg_data = msg.data()
            #print("Received message id={}".format(msg.message_id()))
            #print("Received message '{}' id='{}'".format(msg_data, msg.message_id()))

            # Acknowledge successful processing of the message
            # This is set up to do batch acknowledgement, every 1000 messages
            msgs_recvd += 1
            if msgs_recvd == 1000:
                consumer.acknowledge_cumulative(msg)
                #reset counter for batch acknowledgement after the next 1000 msgs
                msgs_recvd = 0

            #Extract: parse the JSON messages
            msg_json_str = msg_data.decode('utf8')
            msg_cols = json.loads(msg_json_str)

            #Transform: send relevant data to function
            insertion_list = make_insertion_list(msg_cols['country_code'],
                msg_cols['device_id'], msg_cols['device_t'],
                msg_cols['x'], msg_cols['y'], msg_cols['z'])

            #Load
            if (len(insertion_list) > 0):
                insert_list(insertion_list)
            
        except:
            # Message failed to be processed
            print('failed to process')
            consumer.negative_acknowledge(msg)

    client.close()

#test inserting some data
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

    #begin listening
    listen(consumer)

    #close
    conn.close()

if __name__ == "__main__":
    main()
