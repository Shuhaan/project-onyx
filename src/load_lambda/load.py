import boto3, os
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
from dotenv import load_dotenv
from load_utils import log_message, read_parquet_from_s3, \
    write_df_to_warehouse, get_secret


load_dotenv()

def load(bucket="onyx-processed-data-bucket", s3_client=None):
    log_message(__name__, 10, "Entered load function")
    if not s3_client:
        s3_client = boto3.client("s3")
    
    # this try and accept checks for last load time-stamp
    try:
        last_load_file = s3_client.get_object(
            Bucket=bucket, Key="last_load.txt"
        )
        last_load = last_load_file["Body"].read().decode("utf-8")
        log_message(__name__, 20, f"Load function last ran at {last_load}")
    except s3_client.exceptions.NoSuchKey:
        last_load = None
        log_message(__name__, 20, "Load function running for the first time")

    # check for new parquet files since last upload. Need to figure out this logic
    
    read_parquet = read_parquet_from_s3(s3_client, last_load, bucket)
    write_df_to_warehouse(read_parquet)
    
    
    date = datetime.now()
    store_last_load = date.strftime("%Y-%m-%d %H:%M:%S")
    s3_client.put_object(
        Bucket=bucket, Key="last_load.txt", Body=store_last_load
    )
     
    
