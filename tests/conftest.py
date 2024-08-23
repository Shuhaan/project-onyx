import pytest, boto3, os
from moto import mock_aws


# scope="function"
@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_client(aws_credentials):
    with mock_aws(aws_credentials):
        yield boto3.client("s3")


@pytest.fixture()
def s3_data_buckets(s3_client):
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

@pytest.fixture()
def secretsmanager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")

