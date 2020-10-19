import pulsar
import time

from import_data import parse_device_list

#pulsar broker we will be sending data to
broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'

#objects store 5 minutes' worth of data, with these possible names
#(these are minute numbers ranging from 0 to 60, at 5 minute intervals)
obj_name_list = ['00.jsonl', '05.jsonl',
                 '10.jsonl', '15.jsonl',
                 '20.jsonl', '25.jsonl',
                 '30.jsonl', '35.jsonl',
                 '40.jsonl', '45.jsonl',
                 '50.jsonl', '55.jsonl']

YEAR = '2020'                                                                      
MONTH = '09'   

#set and format date parameters we want to use for sending the data
#this is because we don't want to send all available data
#(this is similar to a function in import_data.py, and could possibly be merged)
def set_loop_vars():
    #make lists of months, days, and hours, to iterate through
    global months_list, days_list, hours_list
    months_list = list(range(1,13))
    days_list = list(range(1,11))
    hours_list = list(range(0,24))
    
    #convert to 0-padded strings, as required by the bucket setup
    for i in range(0, len(months_list)):
        months_list[i] = '{0:02}'.format(months_list[i])
    for i in range(0, len(days_list)):
        days_list[i] = '{0:02}'.format(days_list[i])
    for i in range(0, len(hours_list)):
        hours_list[i] = '{0:02}'.format(hours_list[i])

#function called for asynchronous sending        
def callback(res, msg):
    #do not need to do anything with the acknowledgement
    return

#send the data to pulsar
def send_data():
    files_downloaded = 0
    client = pulsar.Client(broker1_url)
    producer = client.create_producer(topic='sensors')

    #store the start time
    t0 = time.time()
    #iterate through the downloaded files and send    
    for day in days_list:
        for hour in hours_list:
            for obj_name in obj_name_list:
                print('day: {}, hour: {}, time: {}'.format(day, hour, obj_name))
                for device_id in device_id_list:   
                    try:
                        #open file
                        download_file_name = '../input_data/device{}_yr{}_mon{}_day{}_hr{}_{}'.format(device_id, YEAR, MONTH, day, hour, obj_name)
                        file = open(download_file_name,'r').read()
                        file_lines = file.splitlines()

                        #file_lines = range(1,300)
                        #test_str = 'hi'.encode()
                        for line in file_lines:
                            #print(download_file_name)
                            #print(line)
                            #producer.send(test_str)

                            #send the data asynchronously (without waiting for acknowledgement)
                            producer.send_async(line.encode('utf-8'), callback)

                        files_downloaded += 1

                    except:
                        #if no file is found at this location, skip
                        #this way, the code doesn't fail due to a malformed path
                        continue

    #calculate end time
    t1 = time.time()
    send_time = t1-t0

    #calculate message velocity
    #each message has 300 lines (5 minutes = 300 seconds)
    msgs_per_file = 300
    print('NUM FILES SENT: {}'.format(files_downloaded))
    print('SEND TIME: {}'.format(send_time))
    print('THROUGHPUT (MSGS/SEC): {}'.format(files_downloaded *
                                             msgs_per_file / send_time))

    #close pulsar client
    client.close()

def main():
    #set parameters
    global device_id_list
    set_loop_vars()
    device_id_list = parse_device_list()

    #loop through the downloaded data and send
    send_data()

if __name__ == "__main__":
    main()
