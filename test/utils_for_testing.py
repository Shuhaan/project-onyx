import boto3
import json
import os
from botocore.exceptions import ClientError


def create_s3_bucket(bucket_name, region="eu-west-2", s3_resource=boto3.resource("s3")):
    return s3_resource.Bucket(bucket_name)


def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)
    return bucket.upload_file(file_name, object_name)


def view_bucket_contents(my_bucket):
    bucket_list = []
    print(my_bucket.objects.all[1])
    for my_bucket_object in my_bucket.objects.all():
        bucket_list.append(my_bucket_object.key)
        print(my_bucket_object.get()["Body"].read().decode("utf-8"))

    print("My bucket contains: ", bucket_list)
    return bucket_list


def credentials_storer(secret_identifier, userId, password, client):
    SecretString = json.dumps({"username": f"{userId}", "password": f"{password}"})
    response = client.create_secret(Name=secret_identifier, SecretString=SecretString)
    print(response["Name"])


def secret_retriever(secret_identifier, client):
    secret_name = secret_identifier
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)
