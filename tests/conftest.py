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
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture()
def s3_data_buckets(s3_client):
    print("creating test-ingested-bucket")
    s3_client.create_bucket(
        Bucket="test-ingested-bucket",
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
    return s3_client
