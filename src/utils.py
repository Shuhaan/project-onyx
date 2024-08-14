import boto3
import os
import json
from botocore.exceptions import ClientError
import logging


def get_secret(secret_name="project-onyx/totesys-db-login", region_name="eu-west-2"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response["SecretString"]
    result_dict = json.loads(secret)
    return result_dict


def log_message(name, level, message=""):
    # sends a message to the logger
    # use __name__ to pass function name to this function
    # example useage: log_message("function_name", "30", "This is a warning")
    # function author: Arif Syed

    logger = logging.getLogger(name)
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%d/%m/%Y %I:%M:%S %p",
    )
    logger.setLevel(logging.DEBUG)

    if level == "10":
        logger.debug(message)

    if level == "20":
        logger.info(message)

    if level == "30":
        logger.warning(message)

    if level == "40":
        logger.error(message)

    if level == "50":
        logger.critical(message)
