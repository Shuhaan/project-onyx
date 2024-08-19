import pytest, json  # , boto3

# from moto import mock_aws
from unittest.mock import patch
from datetime import datetime
from extract_lambda.extract import extract


class TestExtract:
    def test_extract_writes_all_tables_to_s3_as_directories(
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        create_secrets,
    ):

        extract("test_ingested_bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]
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
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        create_secrets,
    ):

        extract("test_ingested_bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if not key.endswith(".txt"):  # Filter out .txt files
                json_file = s3_client.get_object(Bucket="test_ingested_bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    assert content[folder][0]["last_updated"]

    def test_extract_writes_jsons_into_s3_with_correct_data_type_from_db(
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        create_secrets,
    ):

        extract("test_ingested_bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if ".txt" not in key:
                json_file = s3_client.get_object(Bucket="test_ingested_bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    value = content[folder][0]["last_updated"]
                    date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    assert isinstance(date, datetime)

    # @patch("pg8000.native.Connection", return_value=MockedConnection())
    def test_mocked_connection_patch_working(
        self, patch_db_connection, aws_credentials, s3_client, s3_data_buckets
    ):
        extract("test_ingested_bucket", s3_client)

        result_list_bucket = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]

        result = [bucket["Key"] for bucket in result_list_bucket]

        extract("test_ingested_bucket", s3_client)

        result_list_bucket2 = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]

        result2 = [bucket["Key"] for bucket in result_list_bucket2]

        # print(result, "result")
        for key in result:
            if ".txt" not in key:
                json_file = s3_client.get_object(Bucket="test_ingested_bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    assert content[folder][0]["meaningful_data"]
