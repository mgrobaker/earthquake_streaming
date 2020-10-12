import pulsar
import time

broker1_url = 'pulsar://ec2-18-223-193-14.us-east-2.compute.amazonaws.com:6650'

#initialize bucket variables
#TODO: i have to iterate through all the JSON files. not just 00
OBJ_NAME = '00.jsonl'

YEAR = '2020'
device_id = '010'

def set_loop_vars():
    #make lists of months, days, and hours, to iterate through
    global months_list, days_list, hours_list
    months_list = list(range(1,13))
    #days_list = range(1,31)
    days_list = list(range(1,4))
    hours_list = list(range(0,24))
    
    #convert to 0-padded strings, as required by openeew
    for i in range(0, len(months_list)):
        months_list[i] = '{0:02}'.format(months_list[i])
    for i in range(0, len(days_list)):
        days_list[i] = '{0:02}'.format(days_list[i])
    for i in range(0, len(hours_list)):
        hours_list[i] = '{0:02}'.format(hours_list[i])

def send_data():
    files_downloaded = 0
    client = pulsar.Client(broker1_url)
    #producer = client.create_producer(topic='sensors', schema=JSONSchema())
    producer = client.create_producer(topic='sensors')

    t0 = time.time()
    #iterate through the downloaded files and send
    for day in days_list:
        #print('day: {}'.format(day))
        for hour in hours_list:
            try:
                #open file
                download_file_name = '../input_data/device{}_yr{}_mon{}_day{}_hr{}_{}'.format(device_id, YEAR, months_list[8], day, hour, OBJ_NAME)
                file = open(download_file_name,'r').read()
                file_lines = file.splitlines()
                
                for line in file_lines:
                    #print(download_file_name)
#                    print(line)
                    producer.send(line.encode('utf-8'))
                    #producer.send('hi'.encode())

                files_downloaded += 1

            except:
                #if no file is found at this location, skip
                #this way, the code doesn't fail due to a malformed path
                continue

    t1 = time.time()
    send_time = t1-t0

    msgs_per_file = 300
    print('NUM FILES SENT: {}'.format(files_downloaded))
    print('SEND TIME: {}'.format(send_time))
    print('THROUGHPUT (MSGS/SEC): {}'.format(files_downloaded *
                                             msgs_per_file / send_time))
    
    client.close()

def main():
    set_loop_vars()
    send_data()

if __name__ == "__main__":
    main()
