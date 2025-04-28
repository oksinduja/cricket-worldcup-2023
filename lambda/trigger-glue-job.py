import boto3
import json
import time
import os
from botocore.exceptions import ClientError

# Initialize AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Configuration
BUCKET_NAME = 'worldcup-data-2023'  # without s3:// prefix
PREFIX = 'raw/'
DIMENSION_TABLE_JOB_NAME = 'dim_tables_builder'
FACT_TABLE_JOB_NAME = 'fact_table_builder'
REQUIRED_FILES = ['points_table.csv', 'matches.csv', 'deliveries.csv', 'stadiums.json']

def check_files_exist(bucket, prefix, required_files):
    """Check if all required files exist in the specified S3 bucket and prefix."""
    try:
        # List objects in the bucket with the given prefix
        print(f"Checking files in {bucket}/{prefix}")
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"No files found in {bucket}/{prefix}")
            return False
        
        # Extract file names from the response
        files_in_bucket = [obj['Key'] for obj in response['Contents']]
        
        # Check if all required files are present
        all_files_present = True
        for required_file in required_files:
            file_path = f"{prefix}{required_file}"
            if file_path not in files_in_bucket:
                print(f"Required file {file_path} not found in bucket")
                all_files_present = False
        
        if all_files_present:
            print(f"All required files found in {bucket}/{prefix}")
        
        return all_files_present
        
    except ClientError as e:
        print(f"Error checking files in S3: {e}")
        return False

def start_glue_job(job_name):
    """Start a Glue job with the specified name."""
    try:
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Started Glue job {job_name} with run ID: {job_run_id}")
        return job_run_id
    except ClientError as e:
        print(f"Error starting Glue job {job_name}: {e}")
        return None

def wait_for_job_completion(job_name, job_run_id, max_wait_time_minutes=60):
    """Wait for a Glue job to complete and return its status."""
    try:
        max_attempts = max_wait_time_minutes * 2  # Check every 30 seconds
        attempts = 0
        
        while attempts < max_attempts:
            response = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
            status = response['JobRun']['JobRunState']
            
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                print(f"Glue job {job_name} finished with status: {status}")
                return status == 'SUCCEEDED'
            
            print(f"Glue job {job_name} is still running with status: {status}")
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            attempts += 1
        
        print(f"Exceeded maximum wait time for Glue job {job_name}")
        return False
    except ClientError as e:
        print(f"Error waiting for Glue job {job_name}: {e}")
        return False

def lambda_handler(event, context):
    """
    Lambda handler to check for required CSV files and trigger Glue jobs.
    This function gets triggered by S3 events when files are added to the bucket.
    """
    try:
        print(f"Checking for required files in {BUCKET_NAME}/{PREFIX}")
        
        # Check if all required files exist
        files_exist = check_files_exist(BUCKET_NAME, PREFIX, REQUIRED_FILES)
        
        if not files_exist:
            print("Not all required files are present. Exiting.")
            return {
                'statusCode': 200,
                'body': json.dumps('Required files not found. No jobs triggered.')
            }
        
        print("All required files found. Starting ETL jobs sequence.")
        
        # Start the dimension table builder job
        dimension_job_id = start_glue_job(DIMENSION_TABLE_JOB_NAME)
        if not dimension_job_id:
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to start dimension table builder job.')
            }
        
        # Wait for the dimension table job to complete
        dimension_success = wait_for_job_completion(DIMENSION_TABLE_JOB_NAME, dimension_job_id)
        
        if not dimension_success:
            return {
                'statusCode': 500,
                'body': json.dumps('Dimension table builder job failed or timed out.')
            }
        
        print("Dimension table job completed successfully. Starting fact table job.")
        
        # Start the fact table builder job
        fact_job_id = start_glue_job(FACT_TABLE_JOB_NAME)
        if not fact_job_id:
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to start fact table builder job.')
            }
        
        print("Fact table job started successfully.")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully triggered dimension and fact table builder jobs.')
        }
        
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }