import pandas as pd
import boto3, logging, json
from botocore.exceptions import ClientError
# from typing import List, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from io import BytesIO
from datetime import datetime



def get_secret(
    secret_name: str = "project-onyx/warehouse-db-login", 
    region_name: str = "eu-west-2"):
    """
    Retrieves warehouse login credentials from AWS Secrets Manager and 
    returns it as a sqlalchemy engine db string

    :param secret_name: The name of the secret to retrieve.
    :param region_name: The AWS region where the secret is stored.
    :raises ClientError: If there is an error retrieving the secret.
    :return: The secret DB string
    """
    log_message(__name__, 10, "Entered get_secret_for_warehouse")
    try:
        client = boto3.client(service_name="secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_dict = get_secret_value_response["SecretString"]
        secret = json.loads(secret_dict)

        #! secret['engine'] > secret['HOST']

        result = f"postgresql+pg8000://{secret['username']}:{secret['password']}@{secret['engine']}:{secret['port']}/{secret['dbname']}"
        return result

    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        log_message(__name__, "40", e.response["Error"]["Message"])
        raise e

def log_message(name: str, level: int, message: str = ""):
    """
    Sends a message to the logger at a specified level.

    :param name: The name of the logger.
    :param level: The logging level (one of 10, 20, 30, 40, 50).
:param message: The message to log.
    """
    logger = logging.getLogger(name)

    # Define a mapping of level integers to logging methods
    level_map = {
        10: logger.debug,
        20: logger.info,
        30: logger.warning,
        40: logger.error,
        50: logger.critical,
    }

    # Get the logging method from the map
    log_method = level_map.get(level)

    if log_method:
        log_method(message)
    else:
        logger.error("Invalid log level: %d", level)


def read_parquets_from_s3(s3_client, last_load, bucket="onyx-processed-data-bucket"):
    """Reads parquet files from an s3 bucket and converts to pandas dataframe

    Args:
        s3_client (boto3('s3')):    Boto3 s3 client to access s3 bucket
        last_load (string):         read from .txt file containing timestamp 
                                    when load function last ran
        bucket (str, optional):     The name of the s3 bucket to be read.
                                    Defaults to "onyx-processed-data-bucket".
        
    Returns:
        list: list nested with  - list of dim table names
                                - list of fact table name
                                - list of dim dataframes
                                - list of fact dataframes

    """    
    bucket_contents = s3_client.list_objects(Bucket=bucket)["Contents"]
    dim_table_names = [obj['Key'].split('.')[0] for obj in bucket_contents
                       if not '.txt' in obj['Key'] and "dim_" in obj['Key']]
    fact_table_names = [obj['Key'].split('.')[0] for obj in bucket_contents
                        if not '.txt' in obj['Key'] and "fact_" in obj['Key']]
    last_mod = bucket_contents[0]['LastModified']
    print(last_mod)
    last_load = datetime.strptime(last_load,"%Y-%m-%d %H:%M:%S%z")
    print(last_load)
    print(last_load and last_load < last_mod)
    if last_load and last_load < last_mod:
        dim_parquet_files_list = [file_data["Key"] for file_data in bucket_contents
                                  if not '.txt' in file_data['Key'] and "dim_" in file_data['Key']]
        fact_parquet_files_list = [file_data["Key"] for file_data in bucket_contents
                                   if not '.txt' in file_data['Key'] and "fact_" in file_data['Key']]
        dim_df_list = []
        for parquet_file_name in dim_parquet_files_list:   
            response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
            data = response['Body'].read()
            df = pd.read_parquet(BytesIO(data))
            dim_df_list.append(df)
        fact_df_list = []
        for parquet_file_name in fact_parquet_files_list:   
            response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
            data = response['Body'].read()
            df = pd.read_parquet(BytesIO(data))
            fact_df_list.append(df)
        return (dim_table_names, fact_table_names, dim_df_list, fact_df_list)
            

def write_df_to_warehouse(read_parquet, engine_string=None):
    """
    Summary:
    receives lists of table names, and lists of dataframes and writes the
    dataframes to the associated table in a postgres database using 
    sqlalchemy connection. dim tables are written before fact tables

    Args:
        read_parquet (lsit):    list of lists received via output of 
                                read_parquets_from_s3 function
        engine_string (string, optional): database credentials in 
        sqlalchemy db string format received from AWS secrets manager via 
        output of get_secret function. Defaults to None.
    """    
    if not engine_string:
        engine_string = get_secret()
    dim_table_names, fact_table_names, dim_df_list, fact_df_list = read_parquet
    # get db credentials from secrets as engine string
    engine = create_engine(engine_string)
    try:    
        for i in range(len(dim_df_list)):
            dim_df_list[i].to_sql(dim_table_names[i], engine,
                                if_exists='append', index=False)   
        for i in range(len(fact_df_list)):
            fact_df_list[i].to_sql(fact_table_names[i], engine,
                                if_exists='append', index=False)
    except SQLAlchemyError as e:
        log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")
