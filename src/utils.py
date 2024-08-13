import boto3
import json
from botocore.exceptions import ClientError
from datetime import date, datetime
from decimal import Decimal

def get_secret():
    secret_name = "project-onyx/totesys-db-login"
    region_name = "eu-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response['SecretString']
    result_dict = json.loads(secret)
    return result_dict

def format_response(columns, response):
    formatted_response = []
    for row in response:
        extracted_from_response = {}
        for i in range(len(columns)):
            if isinstance(row[i], datetime):
                row[i] = row[i].strftime("%Y-%m-%d %H:%M:%S")
            if type(row[i]) == type(Decimal('1.00')):
                row[i] = float(row[i])
            extracted_from_response[columns[i]] = row[i]
        formatted_response.append(extracted_from_response)
    return formatted_response

def format_response_2(columns, data, label):
    if len(data) > 1:
        formatted_data = [dict(zip(columns, row))for row in data]
    else:
        formatted_data = dict(zip(columns, data[0]))
    return {label: formatted_data}