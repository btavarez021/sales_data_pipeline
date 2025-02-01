import os
import boto3
from botocore.exceptions import ClientError
import re

def get_s3_client():
    s3_client = boto3.client('s3',aws_access_key_id='AKIA4RCAN7ZLYAGQ3DR2',
    aws_secret_access_key='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')

    return s3_client

def upload_file(local_dir, bucket_name, s3_client):
    

    for filename in os.listdir(local_dir):
        if os.path.isfile(os.path.join(local_dir, filename)) and filename.endswith('csv'):
            local_filepath = os.path.join(local_dir, filename)
            
            s3_key = f'sales_data/{filename}'
            try:
                #upload file
                s3_client.upload_file(local_filepath, bucket_name, s3_key)
                print(f"Uploaded {filename} to {bucket_name}/{s3_key}")
            except ClientError as e:
                print(str(e))

def get_files(bucket_name, s3_client):

    file_names = []

    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name,StartAfter='2018')
    for page in result:
        if "Contents" in page:
            for key in page[ "Contents" ]:
                keyString = key[ "Key" ]
                file_name = re.findall(r'[^/]+$', keyString)
                file_names.append(file_name)
                
    return file_names

def delete_files(bucket_name, file_names, s3_client):
    objects_to_delete = []

    for file_name in file_names:
        if isinstance(file_name, list) and file_name:
            file_name = file_name[0]
        s3_key = f'sales_data/{file_name}'
        objects_to_delete.append({'Key': s3_key})
        print("Objects: ", objects_to_delete)
    
    if objects_to_delete:
        response = s3_client.delete_objects(Bucket=bucket_name, Delete={
        'Objects': objects_to_delete
        })
        print(f"Deleted files: {response}")
    else:
        print("No valid objects to delete")

if __name__ == "__main__":
    s3_client = get_s3_client()
    local_directory = '/mnt/c/Users/Benny/projects/sales_data_pipeline'
    bucket = 'company-sales-data-pipeline'

    # Example usage
    upload_file(local_directory, bucket, s3_client)
    files_names = get_files(bucket, s3_client)
    delete_files(bucket, files_names, s3_client)