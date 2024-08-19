import pytest, json
from unittest.mock import patch
from datetime import datetime
from extract_lambda.extract import extract


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
            ["1", "old_data1", "1970-01-01 00:00:00"],
            ["2", "old_data2", "1970-01-01 00:00:00"],
        ]

        self.rows_data2 = [
            ["1", "new_data1", "2024-08-19 20:00:00"],
            ["2", "old_data2", "1970-01-01 00:00:00"],
        ]

    def run(self, query):
        if "WHERE" in query:
            return self.rows_data2
        return self.rows_data1

    def close(self):
        pass


class TestExtract:
    def test_extract_writes_all_tables_to_s3_as_directories(
        self, aws_credentials, s3_client, s3_data_buckets
    ):
        extract("test-ingested-bucket", s3_data_buckets)
        result_list_bucket = s3_data_buckets.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
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
        self, aws_credentials, s3_client, s3_data_buckets
    ):
        extract("test-ingested-bucket", s3_data_buckets)
        result_list_bucket = s3_data_buckets.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if not key.endswith(".txt"):  # Filter out .txt files
                json_file = s3_data_buckets.get_object(
                    Bucket="test-ingested-bucket", Key=key
                )
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    assert content[folder][0]["last_updated"]

    def test_extract_writes_jsons_into_s3_with_correct_data_type_from_db(
        self, aws_credentials, s3_client, s3_data_buckets
    ):
        extract("test-ingested-bucket", s3_data_buckets)
        result_list_bucket = s3_data_buckets.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]
        for key in result:
            if not key.endswith(".txt"):
                json_file = s3_data_buckets.get_object(
                    Bucket="test-ingested-bucket", Key=key
                )
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                for folder in content:
                    value = content[folder][0]["last_updated"]
                    date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    assert isinstance(date, datetime)

    @patch("extract_lambda.extract.connect_to_db", return_value=MockedConnection())
    def test_mocked_connection_patch_working(self, s3_data_buckets):
        # Execute the extraction function
        extract("test-ingested-bucket", s3_data_buckets)

        # List objects in the bucket
        result_list_bucket = s3_data_buckets.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        result = [bucket["Key"] for bucket in result_list_bucket]

        # Process each file and validate its content
        for key in result:
            if not key.endswith(".txt"):
                json_file = s3_data_buckets.get_object(
                    Bucket="test-ingested-bucket", Key=key
                )
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)

                # Validate that 'meaningful_data' is present and correct
                for folder in content:
                    assert "meaningful_data" in content[folder][0]
                    assert content[folder][0]["meaningful_data"] in [
                        "new_data1",
                        "new_data2",
                    ]
