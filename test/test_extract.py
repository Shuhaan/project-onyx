import pytest
import json
from moto import mock_aws
from unittest.mock import patch  # , MagicMock
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv
from src.extract import extract_from_db_write_to_s3

@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture()
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture()
def s3_ingested_data_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="bucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
            "Location": {"Type": "AvailabilityZone", "Name": "string"},
        },
    )
    return s3_client

@pytest.fixture()
def secretsmanager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="function")
def create_secrets(secretsmanager_client):
    load_dotenv()
    secret_string = {
        "username": os.getenv("Username"),
        "password": os.getenv("Password"),
        "host": os.getenv("Hostname"),
        "port": os.getenv("Port"),
        "dbname": os.getenv("Database"),
    }
    secret = json.dumps(secret_string)
    secretsmanager_client.create_secret(
        Name="project-onyx/totesys-db-login", SecretString=secret
    )


class MockedConnection:
    def __init__(
        self,
        user=os.getenv("Username"),
        password=os.getenv("Password"),
        database=os.getenv("Database"),
        host=os.getenv("Hostname"),
        port=os.getenv("Port"),
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
        if 'WHERE' in query:
            # print('Mock WHERE query ran')
            return self.rows_data2
        # print('Mock query ran')
        return self.rows_data1

    def close(self):
        pass


class TestExtract:
    def test_extract_writes_all_tables_to_s3_as_directories(
        self, s3_client, s3_ingested_data_bucket, create_secrets
    ):

        extract_from_db_write_to_s3("bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
        result = [file_data["Key"] for file_data in result_list_bucket]
        expected = [
            "counterparty",
            "currency",
            "department",
            "design",
            "staff",
            "sales_order",
            "address",
            "payment",
            "purchase_order",
            "payment_type",
            "transaction",
            "last_extract",
        ]

        for table in expected:
            assert any([folder.startswith(table) for folder in result])

    def test_extract_writes_jsons_into_s3_with_correct_structure_from_db(
        self, s3_client, s3_ingested_data_bucket, create_secrets
    ):

        extract_from_db_write_to_s3("bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if not key.endswith(".txt"):  # Filter out .txt files
                json_file = s3_client.get_object(Bucket="bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    assert content[folder][0]["created_at"]

    def test_extract_writes_jsons_into_s3_with_correct_data_type_from_db(
        self, s3_client, s3_ingested_data_bucket, create_secrets
    ):

        extract_from_db_write_to_s3("bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if ".txt" not in key:
                json_file = s3_client.get_object(Bucket="bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    value = content[folder][0]["last_updated"]
                    date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    assert isinstance(date, datetime)

                    
    @patch('src.extract.connect_to_db', return_value=MockedConnection())
    def test_mocked_connection_patch_working(
        self, patched_conn, s3_ingested_data_bucket, aws_credentials
    ):
        extract_from_db_write_to_s3("bucket", s3_ingested_data_bucket)
        first_file = s3_ingested_data_bucket.get_object(Bucket='bucket', Key='last_key.txt')['Body'].read().decode('utf-8')
        # print(first_file)
        result_list_bucket = s3_ingested_data_bucket.list_objects(Bucket="bucket")['Contents']
        # print(result_list_bucket)
        
        
        
        extract_from_db_write_to_s3("bucket", s3_ingested_data_bucket)
        second_file = s3_ingested_data_bucket.get_object(Bucket='bucket', Key='last_key.txt')['Body'].read().decode('utf-8')
        # print(second_file)
        result_list_bucket = s3_ingested_data_bucket.list_objects(Bucket="bucket")['Contents']
        # print(result_list_bucket)
        
        

        result = [bucket["Key"] for bucket in result_list_bucket]
        # print(result)
        for key in result:
            # print(key)
            if ".txt" not in key:
                json_file = s3_ingested_data_bucket.get_object(Bucket="bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    print(content[folder][0])
                    assert content[folder][0]["meaningful_data"]
               