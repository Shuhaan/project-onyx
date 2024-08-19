import pytest, boto3, os, json
from moto import mock_aws
from dotenv import load_dotenv

# scope="function"
@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture()
def s3_data_buckets(s3_client):
    print("creating test_injested_bucket")
    s3_client.create_bucket(
        Bucket="test_injested_bucket",
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
            }
        )
    s3_client.create_bucket(
        Bucket="test_processed_bucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
        },
    )
    # return s3_client


@pytest.fixture()
def secretsmanager_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture()
def create_secrets(secretsmanager_client):
    # load_dotenv()
    secret_string = {
        "USERNAME": "user",#os.getenv("USERNAME"),
        "PASSWORD": "pass",#os.getenv("PASSWORD"),
        "HOST": "host",#os.getenv("HOST"),
        "PORT": 5432,#os.getenv("PORT"),
        "DBNAME": "db"#os.getenv("DATABASE"),
    }
    secret = json.dumps(secret_string)
    secretsmanager_client.create_secret(
        Name="project-onyx/totesys-db-login", SecretString=secret
    )