import pandas as pd
import boto3, logging, json
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from sqlalchemy import create_engine

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


def read_parquet_from_s3(bucket_name, s3_client):
    bucket_contents = s3_client.list_objects(
            Bucket="onyx-processed-data-bucket"
        )["Contents"]
    bucket_files = [file_data["Key"] for file_data in bucket_contents]
    list_of_parquets = []
    for file in bucket_files:    
        list_of_parquets.append(s3_client.get_object(Bucket=bucket_name, Key=file))
    return list_of_parquets


def write_df_to_warehouse(parquet_file):
    
    df = pd.read_parquet("dim_currency.parquet")
    table_name = "test_table"
    engine = create_engine('postgresql+pg8000://postgres:password@localhost:5432/load_test')
    df.to_sql(table_name, engine, if_exists='replace', index=False)


write_df_to_warehouse()