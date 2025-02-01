from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import logging
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError
import re
import base64


def get_s3_client():
    s3_client = boto3.client('s3',aws_access_key_id='AKIA4RCAN7ZLYAGQ3DR2',
    aws_secret_access_key='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')

    return s3_client

def upload_s3_files(local_dir, bucket_name):

    # Create S3 client inside the task function
    s3_client = get_s3_client()

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

def get_s3_files(bucket_name, s3_client):

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

def del_s3_files(bucket_name, file_names, s3_client):
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

# Your Fivetran API credentials and connector details
FIVETRAN_API_KEY = "EIpnU1lnKHgBR0jJ"
FIVETRAN_API_SECRET = "QlxQuWIXKBkVvWPATKfNF9O40fqTMAif"
FIVETRAN_CONNECTOR_ID = ["gating_forefront","adjoining_defiant","ditches_reforest"]
encoded_credentials = base64.b64encode(f"{FIVETRAN_API_KEY}:{FIVETRAN_API_SECRET}".encode()).decode()

def trigger_fivetran_sync(api_key, connector_id):

    for id in FIVETRAN_CONNECTOR_ID:
        url = f"https://api.fivetran.com/v1/connectors/{id}/force"
        headers = {
            "Authorization":f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, headers=headers)
            response.raise_for_status()
            print(f"Sync triggered successfully: {response.status_code}")
            print(f"Response: {response.json()}")  # Print the JSON response for more context
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            if response is not None:
                print(f"Response Content: {response.content.decode()}")
        except requests.exceptions.RequestException as e:
            print(f"Error triggering Fivetran sync: {str(e)}")


with DAG(dag_id="sales_data_dag", start_date=datetime(2025,1,27),schedule="0 0 * * *") as dag:

     # Task to get files from S3
    get_s3_files_task = PythonOperator(
        task_id="get_s3_files",
        python_callable=get_s3_files,
        op_kwargs={"bucket_name": "company-sales-data-pipeline",
            "s3_client": get_s3_client()},
    )

    del_s3_files_task = PythonOperator(
        task_id="del_s3_files",
        python_callable=del_s3_files,
        op_kwargs={"bucket_name": "company-sales-data-pipeline",
        "file_names": "{{ task_instance.xcom_pull(task_ids='get_s3_files') }}",
            "s3_client": get_s3_client()},
)

    load_s3_files_task = PythonOperator(
        task_id="load_s3_files",
        python_callable=upload_s3_files,
        op_kwargs={
            "bucket_name": "company-sales-data-pipeline",
            "local_dir": "/mnt/c/Users/Benny/projects/sales_data_pipeline"},
)

    fivetran_task = PythonOperator(
        task_id="fivetran_to_snowflake",
        python_callable=trigger_fivetran_sync,
        op_kwargs={
            "api_key": FIVETRAN_API_KEY, 
            "connector_id": FIVETRAN_CONNECTOR_ID}

    )



    
    execute_dbt_task = BashOperator(
        task_id="execute_dbt",
        bash_command="export DBT_PROFILES_DIR=/mnt/c/Users/Benny/projects/sales_data_pipeline/sales_project && dbt run --project-dir /mnt/c/Users/Benny/projects/sales_data_pipeline/sales_project"
    )

    get_s3_files_task >> del_s3_files_task >> load_s3_files_task >> fivetran_task >> execute_dbt_task
