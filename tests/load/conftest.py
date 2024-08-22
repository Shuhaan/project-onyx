import pytest, boto3, json, os
from dotenv import load_dotenv
from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from moto import mock_aws
from unittest.mock import patch
from load_utils import log_message

load_dotenv()

@pytest.fixture()
def db_credentials_fail(
    user="loser", password="TEST", database="dbz", host="club", port=1234
):
    return (user, password, database, host, port)


@pytest.fixture()
def util_populate_mock_s3(s3_data_buckets):
        test_parquet1 = "tests/load/dim_counterparty.parquet"
        test_parquet2 = "tests/load/dim_currency.parquet"
        test_parquet3 = "tests/load/fact_design.parquet"
        test_time_stamp = "tests/load/time_stamp.txt"
        s3_data_buckets.upload_file(test_parquet3,
                              "test-processed-bucket", "fact_design.parquet")
        s3_data_buckets.upload_file(test_parquet1,
                              "test-processed-bucket", "dim_counterparty.parquet")
        s3_data_buckets.upload_file(test_parquet2,
                              "test-processed-bucket", "dim_currency.parquet")
        
        s3_data_buckets.upload_file(test_time_stamp, "test-processed-bucket", "time_stamp.txt")


@pytest.fixture()
def util_connect_to_mock_warehouse():
      return Connection(
            user=os.getenv("TEST-USER"),
            password=os.getenv("TEST-PASSWORD"),
            database=os.getenv("TEST-DATABASE"),
            # host=credentials["HOST"],
            port=5432,
        )