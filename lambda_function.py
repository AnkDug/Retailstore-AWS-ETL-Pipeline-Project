import boto3
import time
import subprocess
from send_email import send_email
from datetime import datetime, timedelta
import json
import os


def lambda_handler(event, context):
    s3_file_list = []                               #creating an empty list
    
    s3_client=boto3.client('s3')                    #establishing s3 connection
    
    #for loops= to get all the keys present content s3 and append their names to the empty list s3_file_list 
    for object in s3_client.list_objects_v2(Bucket='midtermdata-bucket')['Contents']:
        s3_file_list.append(object['Key'])
    print('S3_file_list:', s3_file_list)
    
    
    
    default_date_str = time.strftime("%Y-%m-%d")    # return the local time zone when code is executed in YYY-MM-DD format to match csv files 
    os.environ['TZ'] = 'America/Edmonton'           # change local timezone to edmonton time
    time.tzset()                                    # tzset() method resets the time conversion rules used by the library routines to new set timezone.
    datestr = time.strftime("%Y-%m-%d")
    #datestr = "2024-03-02"                    
    
    
   #the required file list that needs to be processed in s3 bucket (it uses the date the script is run)
    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']
    print('required_file_list:', required_file_list)
    
    matching_files = [x for x in s3_file_list if x in required_file_list]
    
    # scan S3 bucket
    if set(matching_files)==set(required_file_list):                                    # matching the s3 files with the required files
        s3_file_url = ['s3://' + 'midtermdata-bucket/' + a for a in s3_file_list]     #creating a list compression that create a list for the url to the matching require filespresent in the s3_file_list
        print(s3_file_url)
        table_name = [a[:-15] for a in matching_files]
        print(table_name)
    
        #creating a variable that store json object using json.dumps whcih convert dictionary to json object . 
        #The dictionary was created using dictionary comprenhension which has two for loop that create key_value pair of tablename/filename and url    
        data = json.dumps({'conf':{a:b for a, b in zip(table_name, required_file_list)}})
        print(data)
        
        # send signal to Airflow    
        endpoint= 'http://3.96.120.36:8080/api/v1/dags/a_midterm_dag/dagRuns'
        subprocess.run([
            'curl', 
            '-X', 
            'POST', endpoint, 
            '-H', 'Content-Type: application/json',
            '--user', 'airflow:airflow',
            '--data', data
            ])
        print('File are send to Airflow')
        
    else:
        print("data files not complete")
        send_email()
