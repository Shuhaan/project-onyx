import pandas as pd
import boto3, logging, json
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from sqlalchemy import create_engine
from io import BytesIO

def get_secret(
    secret_name: str = "STILL TO BE WRITTEN", region_name: str = "eu-west-2"
) -> Dict[str, Any]:
    """
    Retrieves a secret from AWS Secrets Manager and parses it as a dictionary.

    :param secret_name: The name of the secret to retrieve.
    :param region_name: The AWS region where the secret is stored.
    :raises ClientError: If there is an error retrieving the secret.
    :return: The secret as a dictionary.
    """
    # Create a Secrets Manager client
    log_message(__name__, 10, "Entered get_secret_for_warehouse")
    try:
        client = boto3.client(service_name="secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        result_dict = json.loads(secret)
        return result_dict

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


def read_parquets_from_s3(s3_client, bucket="onyx-processed-data-bucket"):
    bucket_contents = s3_client.list_objects(Bucket=bucket)["Contents"]
    # print(bucket_contents[0]['Key'],'<<< file 1')
    table_name = bucket_contents[0]['Key'].split('.')[0]
    # print(bucket_contents[0]['LastModified'],'<<< last modified 1')
    # print(bucket_contents[0])
    # print(table_name)
    # time stamp logic ^^^^ can be added to list comprehnsion for filtering
    parquet_files_list = [file_data["Key"] for file_data in bucket_contents]
    # print(parquet_files_list)
    
    df_list = []
    for parquet_file_name in parquet_files_list:   
        response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
        data = response['Body'].read()
        
        df = pd.read_parquet(BytesIO(data))
        df_list.append(df)
    # print(list_of_dfs)
    read_parquet = [table_name, df_list]
    return read_parquet



def write_df_to_warehouse(read_parquet, db_name):
    table_name = read_parquet[0]
    df_list = read_parquet[1]
    # print(table_name)
    # print(df_list)
    # for df in df_list:
        
    #     # db credentials need to be updated with AWS secrets
    #     engine = create_engine(f'postgresql+pg8000://postgres:password@localhost:5432/{db_name}')
    #     df.to_sql(table_name, engine, if_exists='append', index=False)
    print(table_name)
    return table_name


