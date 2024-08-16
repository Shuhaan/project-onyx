import pytest
from moto import mock_aws
from dotenv import load_dotenv
from extract_lambda.extract import extract
from transform_lambda.transform import transform


@pytest.fixture()
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture()
def s3_data_buckets(s3_client):
    s3_client.create_bucket(
        Bucket="onyx-totesys-ingested-data-bucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
            "Location": {"Type": "AvailabilityZone", "Name": "string"},
        },
    )
    s3_client.create_bucket(
        Bucket="onyx-processed-data-bucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
            "Location": {"Type": "AvailabilityZone", "Name": "string"},
        },
    )


@pytest.fixture()
def secretsmanager_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="function")
def create_secrets(secretsmanager_client):
    load_dotenv()
    secret_string = {
        "USERNAME": os.getenv("USERNAME"),
        "PASSWORD": os.getenv("PASSWORD"),
        "HOST": os.getenv("HOST"),
        "PORT": os.getenv("PORT"),
        "DBNAME": os.getenv("DATABASE"),
    }
    secret = json.dumps(secret_string)
    secretsmanager_client.create_secret(
        Name="project-onyx/totesys-db-login", SecretString=secret
    )


@pytest.fixture()
def write_files_to_ingested_date_bucket(create_secrets, s3_data_buckets):
    extract("onyx-totesys-ingested-data-bucket")
