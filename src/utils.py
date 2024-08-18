import boto3
import json
#import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
from decimal import Decimal
import logging


def get_secret(secret_name="project-onyx/totesys-db-login", region_name="eu-west-2"):
    """retrieves database connection credentials held in AWS secrets manager.

    Args:
        secret_name (str, optional): Secret name to be retrieved. Defaults to "project-onyx/totesys-db-login".
        region_name (str, optional): AWS region where secrets manager is located. Defaults to "eu-west-2".

    Raises:
        e: ClientError. Any error in retrieving secret is logged and exception raised.

    Returns:
        Dict: Dictionary containing AWS secrets DB connection details
    """
    # Create a Secrets Manager client
    log_message(__name__, 10, "Entered get_secret")
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


def format_response(columns, response):
    """Formats the raw database query response into a list of dictionaries 
    with column names as keys and row values as dictionary values. Datetime 
    objects are converted to strings

    Args:
        columns (list): List of column names returned by the database query.
        response (list): List of rows from raw database data.

    Returns:
        list: List of dictionaries
    """
    log_message(__name__, 10, "Entered format_response")
    formatted_response = []
    for row in response:
        extracted_from_response = {}
        for i in range(len(columns)):
            if isinstance(row[i], datetime):
                row[i] = row[i].strftime("%Y-%m-%d %H:%M:%S")
            if type(row[i]) == type(Decimal("1.00")):
                row[i] = float(row[i])
            extracted_from_response[columns[i]] = row[i]
        formatted_response.append(extracted_from_response)
    return formatted_response


def log_message(name, level, message=""):
    """
    Sends a message to AWS Cloudwatch.

    Args:
        :name: The name of the logger.
        :level: The logging level (one of 0, 10, 20, 30, 40, 50).
        :message: The message to log.
        
    Returns:
    None
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


# def create_df_from_json(source_bucket, file_name):
#     """_summary_

#     Args:
#         source_bucket (_type_): _description_
#         file_name (_type_): _description_

#     Returns:
#         _type_: _description_
#     """
    # if file_name.endswith(".json"):
    #     s3_client = boto3.client("s3")

    #     table = file_name.split("/")[0]
    #     json_file = s3_client.get_object(Bucket=source_bucket, Key=file_name)
    #     json_contents = json_file["Body"].read().decode("utf-8")
    #     data = json.loads(json_contents).get(table, [])

    #     if not data:  # Skip if the JSON content does not contain expected table data
    #         print(f"No data found for table: {table}")
    #     else:
    #         df = pd.DataFrame(data)
    #         return df
