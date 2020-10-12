import boto3, botocore

#initialize bucket variables
s3 = boto3.resource('s3')
BUCKET_NAME = 'grillo-openeew'
OBJ_NAME = '00.jsonl'
#TODO: i have to iterate through all the JSON files. not just 00

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

def download_data():
    files_downloaded = 0        

    #iterate through the objects in the bucket
    for day in days_list:
        #print('day: {}'.format(day))
        for hour in hours_list:
            BUCKET_PATH = 'records/country_code=mx/device_id={}/year={}/month={}/day={}/hour={}/{}'.format(device_id, YEAR, months_list[8], day, hour, OBJ_NAME)
            #print('HOUR: {}'.format(hour))
            #print('{}'.format(BUCKET_PATH))

            try:
                #download file
                download_file_name = '../input_data/device{}_yr{}_mon{}_day{}_hr{}_{}'.format(device_id, YEAR, months_list[8], day, hour, OBJ_NAME)
                
                s3.Bucket(BUCKET_NAME).download_file(BUCKET_PATH, download_file_name)

                print('downloaded: {}'.format(BUCKET_PATH))
                files_downloaded += 1

            except:
                #if no file is found at this location, skip
                #this way, the code doesn't fail due to a malformed path
                continue

    print('NUM FILES DOWNLOADED: {}'.format(files_downloaded))

def main():
    set_loop_vars()
    download_data()

if __name__ == "__main__":
    main()
