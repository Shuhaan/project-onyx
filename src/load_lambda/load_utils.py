import pandas as pd
import boto3, logging, json, os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime
from decimal import Decimal

load_dotenv()

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


def read_parquets_from_s3(s3_client, last_load, bucket="onyx-processed-data-bucket"):
    bucket_contents = s3_client.list_objects(Bucket=bucket)["Contents"]
    dim_table_names = [obj['Key'].split('.')[0] for obj in bucket_contents
                       if not '.txt' in obj['Key'] and "dim_" in obj['Key']]
    fact_table_names = [obj['Key'].split('.')[0] for obj in bucket_contents
                        if not '.txt' in obj['Key'] and "fact_" in obj['Key']]
    last_mod = bucket_contents[0]['LastModified']
    last_load = datetime.strptime(last_load,"%Y-%m-%d %H:%M:%S%z")
    if last_load < last_mod:
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
            

def write_df_to_warehouse(read_parquet, db_name):
    dim_table_names, fact_table_names, dim_df_list, fact_df_list = read_parquet
    # get db credentials from secrets
    engine = create_engine(f'postgresql+pg8000://{os.getenv("TEST-USER")}:{os.getenv("TEST-PASSWORD")}@localhost:5432/{db_name}')
    for i in range(len(dim_df_list)):
        dim_df_list[i].to_sql(dim_table_names[i], engine, if_exists='append', index=False)   
    for i in range(len(fact_df_list)):
        fact_df_list[i].to_sql(fact_table_names[i], engine, if_exists='append', index=False)    
    # return dim_table_names

def format_response(
    columns: List[str], response: List[List[Any]]
) -> List[Dict[str, Any]]:
    """
    Formats a response into a list of dictionaries with columns as keys.

    Validates that each row in the response has the same number of elements as the columns.

    :param columns: A list of column names.
    :param response: A list of rows, where each row is a list of values.
    :return: A list of dictionaries where each dictionary represents a row.
    :raises ValueError: If any row in the response has a different number of elements than the columns.
    """
    log_message(__name__, 10, "Entered format_response")

    formatted_response = []
    num_columns = len(columns)

    for row in response:
        if len(row) != num_columns:
            raise ValueError("Mismatch between number of columns and row length")

        extracted_from_response = {}

        for column, value in zip(columns, row):
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, Decimal):
                value = float(value)

            extracted_from_response[column] = value

        formatted_response.append(extracted_from_response)

    return formatted_response


