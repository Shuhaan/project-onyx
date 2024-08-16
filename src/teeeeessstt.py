import boto3
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "project-onyx/totesys-db-login"
    region_name = "eu-west-2"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    response = client.list_secrets()
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(e.response["Error"]["Message"])

    secret = get_secret_value_response["SecretString"]

    return secret


print(get_secret())
