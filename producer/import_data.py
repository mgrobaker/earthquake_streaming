import boto3, botocore
import json

#initialize bucket variables
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'
obj_name_list = ['00.jsonl', '05.jsonl',
                 '10.jsonl', '15.jsonl',
                 '20.jsonl', '25.jsonl',
                 '30.jsonl', '35.jsonl',
                 '40.jsonl', '45.jsonl',
                 '50.jsonl', '55.jsonl']

YEAR = '2020'
MONTH = '09'

def set_download_loop_vars():
    #make lists of months, days, and hours, to iterate through
    global months_list, days_list, hours_list
    months_list = list(range(1,13))
    #days_list = range(1,31)
    days_list = list(range(10,15))
    hours_list = list(range(0,24))
    
    #convert to 0-padded strings, as required by openeew
    for i in range(0, len(months_list)):
        months_list[i] = '{0:02}'.format(months_list[i])
    for i in range(0, len(days_list)):
        days_list[i] = '{0:02}'.format(days_list[i])
    for i in range(0, len(hours_list)):
        hours_list[i] = '{0:02}'.format(hours_list[i])

def download_data():
    files_downloaded = 0        

    #iterate through the objects in the bucket
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

                        print('downloaded: {}'.format(BUCKET_PATH))
                        files_downloaded += 1

                    except:
                        #if no file is found at this location, skip
                        #this way, the code doesn't fail due to a malformed path
                        continue

    print('NUM FILES DOWNLOADED: {}'.format(files_downloaded))

def download_device_list():
    file_name = 'devices.jsonl'
    BUCKET_PATH = 'devices/country_code=mx/{}'.format(file_name)

    s3.Bucket(BUCKET_NAME).download_file(
        BUCKET_PATH, '../input_sensor_list/{}'.format(file_name))

def parse_device_list():
    #make list for each col, go thru lines, and append to it
    global device_id_list
    device_id_list = []
    latitude_list = []
    longitude_list = []

    #open file
    file_name = 'devices.jsonl'        
    file = open('../input_sensor_list/{}'.format(file_name),'r').read()
    file_lines = file.splitlines()
    
    for line in file_lines:
        line_cols = json.loads(line)
        if line_cols['is_current_row']:
            #print('device_id: {}'.format(line_cols['device_id']))
            device_id_list.append(line_cols['device_id'])
            latitude_list.append(line_cols['latitude'])
            longitude_list.append(line_cols['longitude'])

    return device_id_list            
            
def main():
    download_device_list()
    parse_device_list()

    set_download_loop_vars()
    download_data()
    
if __name__ == "__main__":
    main()
