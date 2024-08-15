import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime
from decimal import Decimal
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_secret(secret_name="project-onyx/totesys-db-login", region_name="eu-west-2"):
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
    Sends a message to the logger.

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
