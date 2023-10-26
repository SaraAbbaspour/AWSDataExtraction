import time
import boto3
import pandas as pd
import io

'''
%%%%%%%%%%%%%%%%%%%%%% Athena Data Extraction Script %%%%%%%%%%%%%%%%%%%%%%
Sara Abbaspour 08/21/2023
This Python script allows you to easily extract data from Athena using SQL queries. 
The provided SQL query can be customized to download various types of data based on your needs. 
The current configuration of the script focuses on extracting unscripted EV+ signal data. 
The extracted data is automatically saved in separate files for each subject, with two files per subject (left and right).

Prerequisites
Before using this script, ensure you have the following:
1. AWS Credentials: Add your AWS credentials
2. Output Directory: Specify the directory where you want the extracted data to be saved. 
3. Subjects Data File: This script relies on the 'Unscripted_subjects.xlsx' file to retrieve subjects' device IDs and collection dates. Provide the directory path to this file in the script.
'''

# In[]: AWS boto3 conection credentials and database info
aws_access_key_id= ''
aws_secret_access_key = ''
aws_session_token = ''

temp_output_s3_fold = ''

temp_s3_bucket = ''
aws_region = ''

ev_database = ''
athena_db = ev_database

boto_session = boto3.Session(aws_access_key_id= aws_access_key_id, 
                            aws_secret_access_key = aws_secret_access_key,
                            aws_session_token = aws_session_token) 

# In[]: function to extract data from Athena dabase
def athena_query_run(boto_session, region, athena_data_base, athena_query, s3_bucket, s3_output_folder):
    """Runs an Athena query and returns the result in a dataframe"""
    athena_client = boto_session.client('athena', region_name=region)
    s3_client = boto_session.client('s3', region_name=region)
    athena_s3_folder_key = s3_output_folder +'/'
    s3_output = 's3://'+ s3_bucket+'/'+ athena_s3_folder_key
    response = athena_client.start_query_execution(
        QueryString=athena_query,
        QueryExecutionContext={
            'Database': athena_data_base
            },
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    print('Execution ID: ' + response['QueryExecutionId'])
    try:
        query_status = None
        while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
            query_status = athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
            print(query_status)
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                raise Exception('Athena query with the string "{}" failed or was cancelled'.format(athena_query))
            time.sleep(10)
        print('Query "{}" finished.'.format(athena_query))
        aws_key = athena_s3_folder_key + response['QueryExecutionId']+".csv"

        #s3_client.delete_object(Bucket = s3_bucket, Key = aws_key+".metadata")
        print('Athena query output CSV file: '+'s3://'+s3_bucket+'/'+aws_key)
        obj = s3_client.get_object(Bucket=s3_bucket, Key=aws_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    except Exception as e:
        print(e)
        
# In[]: get subject info file
data_dir = ''
subjects_unscripted = data_dir + ''
participants = pd.read_excel(subjects_unscripted)

def find_t0_t1(subject, participants):
    participants['Begin_date'] = pd.to_datetime(participants['Begin_date_time']).dt.date
    participants['End_date'] = pd.to_datetime(participants['End_date_time']).dt.date
    Begin_date = participants[participants['ID'].str[:4]==subject].Begin_date.reset_index(drop=True)
    End_date   = participants[participants['ID'].str[:4]==subject].End_date.reset_index(drop=True)
    DeviceID_left = participants[participants['ID'].str[:4]==subject]['Everion+_Left'].reset_index(drop=True)
    DeviceID_right = participants[participants['ID'].str[:4]==subject]['Everion+_Right'].reset_index(drop=True)
    return Begin_date, End_date, DeviceID_left, DeviceID_right

for i in range(211, 233):
    subject = "U" + str(i)
    print('-----------------------------' + subject + '-----------------------------')
    Begin_date, End_date, DeviceID_left, DeviceID_right = find_t0_t1(subject, participants)
    
    # In[]: athena query
    device_ids_left = "', '".join(list(set(DeviceID_left)))
    device_ids_right = "', '".join(list(set(DeviceID_right)))
    
    unique_dates = list(set([date.strftime('%Y-%m-%d') for date in set(Begin_date) | set(End_date)]))
    date_range = "', '".join(unique_dates)
    
    athena_query = """
    SELECT 
        device_id,
        patient_id,
        record_date,
        signal."TimeStamp",
        signal."MotionActivity",
        signal."Movement",
        signal."NumberOfSteps",
        signal."ActivityClassification"[1] as ActivityClassification, 
        signal."ActivityClassification"[2] as ActivityClassification_qlty
    FROM 
        ""."",
        UNNEST(data) as sys(signal)
    WHERE 
        device_id IN ('{}')
        AND record_date IN ('{}')
    ORDER BY signal."TimeStamp" ASC
    """.format("', '".join([device_ids_left, device_ids_right]), date_range)

    # In[]: ger the output and save 2 files (right and left) per subject
    data_df = athena_query_run(boto_session, aws_region, athena_db, athena_query, temp_s3_bucket, temp_output_s3_fold)
    
    data_df_raw_data_left = data_df[data_df['device_id'].isin(DeviceID_left.tolist())].reset_index(drop=True).copy()
    data_df_raw_data_right = data_df[data_df['device_id'].isin(DeviceID_right.tolist())].reset_index(drop=True).copy()
    
    data_df_raw_data_left.to_csv(data_dir + 'Signal_Data_/' + subject + '_left.csv' )
    data_df_raw_data_right.to_csv(data_dir + '_Signal_Data_/' + subject + '_right.csv' )
