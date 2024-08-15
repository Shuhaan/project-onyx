import pytest
import json
from moto import mock_aws
from unittest.mock import patch
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv
from src.extract import extract_from_db_write_to_s3
from pprint import pprint
from utils_for_testing import (
    create_s3_bucket,
    upload_to_s3,
    view_bucket_contents,
    credentials_storer,
    secret_retriever,
)


load_dotenv()


@pytest.fixture()
def s3_client():
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


@pytest.fixture()
def secretsmanager_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="function")
def create_secrets(secretsmanager_client):
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
    def __init__(self, 
                user=os.getenv("Username"), 
                password=os.getenv("Password"),
                database=os.getenv("Database"),
                host=os.getenv("Hostname"),
                port=os.getenv("Port")
                ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.columns = [
            {"name":"data_id"},
            {"name":"meaningful_data"},
            {"name":"last_updated"},
        ]
        self.data1 = [["1",
                      "old_data1",
                      "1970-01-01 20:00:00"],
                      ["2",
                      "old_data2",
                      "1970-01-01 20:00:00"]]
        
        self.data2 = [["1",
                      "new_data1",
                      "1980-01-01 20:00:00"],
                      ["2",
                      "old_data2",
                      "1970-01-01 20:00:00"]]

    
    def run(self, query):
        


class TestExtract:
    def test_extract_writes_all_tables_to_s3_as_directories(
        self, s3_client, s3_ingested_data_bucket, create_secrets
    ):

        extract_from_db_write_to_s3("bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]
        expected = [
            "address",
            "counterparty",
            "currency",
            "department",
            "design",
            "last_extract.txt",
            "payment",
            "payment_type",
            "purchase_order",
            "sales_order",
            "staff",
            "transaction",
        ]
        for folder, table in zip(result, expected):
            assert table in folder

    def test_extract_writes_jsons_into_s3_with_correct_structure_from_db(
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


    @patch('pg8000.native.Connection', return_value=MockedConnection())      
    def test_extract_only_uploads_new_entries_to_s3(
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
                    assert content[folder][0]["created_at"]