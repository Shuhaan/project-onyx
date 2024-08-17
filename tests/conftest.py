import pytest, boto3, os, json
from moto import mock_aws
from dotenv import load_dotenv


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"



@pytest.fixture(scope="function")
def secretsmanager_client(aws_credentials):
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
        "DBNAME": os.getenv("DATABASE")
    }
    secret = json.dumps(secret_string)
    secretsmanager_client.create_secret(
        Name="project-onyx/totesys-db-login", SecretString=secret
    )


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def s3_data_buckets(s3_client):
    print("creating test-ingested-bucket")
    s3_client.create_bucket(
        Bucket="test-ingested-bucket",
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
            }
        )
    s3_client.create_bucket(
        Bucket="test-processed-bucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
        },
    )
    return s3_client
