import pytest
import json
from moto import mock_aws
import boto3
import os
from dotenv import load_dotenv
from src.extract import extract_from_db_write_to_s3
from src.transform import transform


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


@pytest.fixture()
def write_files_to_ingested_date_bucket(create_secrets, s3_data_buckets):
    extract_from_db_write_to_s3("onyx-totesys-ingested-data-bucket")


class TestTransform:
    @pytest.mark.skip
    def test_transform_puts_files_in_processed_data_bucket(
        self, s3_client, write_files_to_ingested_date_bucket
    ):
        ingested_data_files = s3_client.list_objects(
            Bucket="onyx-totesys-ingested-data-bucket"
        )["Contents"]
        ingested_files = [bucket["Key"] for bucket in ingested_data_files]

        for file in ingested_files:
            transform(
                "onyx-totesys-ingested-data-bucket", file, "onyx-processed-data-bucket"
            )

        result_list_processed_data_bucket = s3_client.list_objects(
            Bucket="onyx-processed-data-bucket"
        )["Contents"]
        result = [bucket["Key"] for bucket in result_list_processed_data_bucket]
        print(result)

        expected = [
            "dim_staff.parquet",
            "dim_location.parquet",
            "dim_design.parquet",
            "dim_date.parquet",
            "dim_currency.parquet",
            "dim_counterparty.parquet",
            "fact_sales_order.parquet",
        ]

        for table in expected:
            assert any([folder.startswith(table) for folder in result])
