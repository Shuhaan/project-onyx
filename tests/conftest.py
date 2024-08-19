import pytest, boto3, os, json
from moto import mock_aws
from dotenv import load_dotenv
from unittest.mock import patch

class MockedConnection:
    def __init__(
        self, user="user", password="pass", database="db", host="host", port=5432
    ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.columns = [
            {"name": "data_id"},
            {"name": "meaningful_data"},
            {"name": "last_updated"},
        ]
        self.rows_data1 = [
            ["1", "old_data1", "1970-01-01 20:00:00"],
            ["2", "old_data2", "1970-01-01 20:00:00"],
        ]

        self.rows_data2 = [
            ["1", "new_data1", "1980-01-01 20:00:00"],
            ["2", "old_data2", "1970-01-01 20:00:00"],
        ]

    def run(self, query):
        if "WHERE" in query:
            return self.rows_data2
        return self.rows_data1

    def close(self):
        pass


# scope="function"
@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def mock_db_connection():
    return MockedConnection()


@pytest.fixture()
def patch_db_connection(mock_db_connection):
    with patch("extract_lambda.extract.connect_to_db", return_value=MockedConnection()):
        yield mock_db_connection


@pytest.fixture()
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture()
def s3_data_buckets(s3_client):
    print("creating test_ingested_bucket")
    s3_client.create_bucket(
        Bucket="test_ingested_bucket",
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
    # print(s3_client.list_buckets())
    return s3_client


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
