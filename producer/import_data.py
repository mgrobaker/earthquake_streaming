import boto3, botocore
import json

#initialize bucket variables
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'

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

#set and format date parameters we want to use for downloading the data
#this is because we don't want to download all available data
def set_download_loop_vars():
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

#download the data for the date parameters we have set
def download_data():
    files_downloaded = 0        

    #iterate through the objects in the buckets
    #we will only get data for mexico (mx)
    for day in days_list:
        #print('day: {}'.format(day))
        for hour in hours_list:
            for obj_name in obj_name_list:
                for device_id in device_id_list:
                    BUCKET_PATH = 'records/country_code=mx/device_id={}/year={}/month={}/day={}/hour={}/{}'.format(device_id, YEAR, MONTH, day, hour, obj_name)
                        #print('HOUR: {}'.format(hour))
                        #print('{}'.format(BUCKET_PATH))

                    try:
                        #download file
                        download_file_name = '../input_data/device{}_yr{}_mon{}_day{}_hr{}_{}'.format(device_id, YEAR, MONTH, day, hour, obj_name)
                        s3.Bucket(BUCKET_NAME).download_file(BUCKET_PATH, download_file_name)
                        
                        #print log message so we can see files are being downloaded
                        print('downloaded: {}'.format(BUCKET_PATH))
                        files_downloaded += 1

                    except:
                        #if no file is found at this location, skip
                        #this way the code doesn't fail due to a malformed path
                        continue

    print('NUM FILES DOWNLOADED: {}'.format(files_downloaded))

#download device metadata
def download_device_list():
    file_name = 'devices.jsonl'
    BUCKET_PATH = 'devices/country_code=mx/{}'.format(file_name)

    s3.Bucket(BUCKET_NAME).download_file(
        BUCKET_PATH, '../input_sensor_list/{}'.format(file_name))

#parse device metadata
def parse_device_list():
    #make list for each col, go thru lines, and append to it
    global device_id_list
    device_id_list = []

    #open file
    file_name = 'devices.jsonl'        
    file = open('../input_sensor_list/{}'.format(file_name),'r').read()
    file_lines = file.splitlines()
    
    for line in file_lines:
        line_cols = json.loads(line)
        #only store device id if is_current_row is True
        if line_cols['is_current_row']:
            #print('device_id: {}'.format(line_cols['device_id']))
            device_id_list.append(line_cols['device_id'])

    return device_id_list            
            
def main():
    #download and parse device metadata
    download_device_list()
    parse_device_list()

    #set date params, and download device data
    set_download_loop_vars()
    download_data()
    
if __name__ == "__main__":
    main()
