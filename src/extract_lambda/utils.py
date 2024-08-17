import boto3, logging, json
from botocore.exceptions import ClientError
from decimal import Decimal
from typing import List, Dict, Any
from datetime import datetime


def get_secret(
    secret_name: str = "project-onyx/totesys-db-login", region_name: str = "eu-west-2"
) -> Dict[str, Any]:
    """
    Retrieves a secret from AWS Secrets Manager and parses it as a dictionary.

    :param secret_name: The name of the secret to retrieve.
    :param region_name: The AWS region where the secret is stored.
    :raises ClientError: If there is an error retrieving the secret.
    :return: The secret as a dictionary.
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


def get_file_contents(key, s3_client):
    json_file = s3_client.get_object(
        Bucket="onyx-totesys-ingested-data-bucket", Key=key
    )
    json_contents = json_file["Body"].read().decode("utf-8")
    content = json.loads(json_contents)
    return content
