import pytest, json
from datetime import datetime
from extract import extract
from pg8000.exceptions import DatabaseError


class TestExtract:
    def test_extract_writes_all_tables_to_s3_as_directories(
        self, s3_data_buckets, patch_db_connection
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

    def extract_test_data(self, client):
        """
        Utility test for extracting data from mock bucket

        :param client (s3_data_buckets): required to give access to the mocked client.

        :return: list: list of dictionaries for the table data.
        """
        extract("test-ingested-bucket", client)
        result_list_from_bucket = client.list_objects(Bucket="test-ingested-bucket")[
            "Contents"
        ]
        result = [obj["Key"] for obj in result_list_from_bucket]
        all_tables = []
        for key in result:
            if not key.endswith(".txt"):  # Filter out last_extract.txt file
                json_file = client.get_object(Bucket="test-ingested-bucket", Key=key)
                json_contents = json_file["Body"].read().decode("utf-8")
                content = json.loads(json_contents)
                all_tables.append(content)
        return all_tables

    def test_extract_writes_jsons_into_s3_with_correct_structure_from_db(
        self, s3_data_buckets, patch_db_connection
    ):
        all_tables = self.extract_test_data(s3_data_buckets)
        for dict_table in all_tables:
            for table_data in dict_table.values():
                assert table_data[0]["last_updated"]

    def test_extract_writes_jsons_into_s3_with_correct_data_type_from_db(
        self, s3_data_buckets, patch_db_connection
    ):

        all_tables = self.extract_test_data(s3_data_buckets)
        for dict_table in all_tables:
            for table_data in dict_table.values():
                value = table_data[0]["last_updated"]
                date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                assert isinstance(date, datetime)

    def test_mocked_connection_patch_working(
        self, s3_data_buckets, patch_db_connection
    ):
        def verify_extract(mock_data):
            """
            Utility test for verifying data from mock bucket.
            Asserts if data in bucket is consistent with mock_data.

            :param mock_data: required to give access to the mocked client.

            :return: None
            """
            extract("test-ingested-bucket", s3_data_buckets)
            result_list_bucket = s3_data_buckets.list_objects(
                Bucket="test-ingested-bucket"
            )["Contents"]
            result = [bucket["Key"] for bucket in result_list_bucket]

            for key in result:
                if "address" in key:
                    json_file = s3_data_buckets.get_object(
                        Bucket="test-ingested-bucket", Key=key
                    )
                    json_contents = json_file["Body"].read().decode("utf-8")
                    assert (
                        json.loads(json_contents)["address"][0]["meaningful_data"]
                        == mock_data
                    )

        verify_extract("old_data1")
        verify_extract("new_data1")


class TestConnection:
    def test_connection_returns_mocked_connection(self, patch_db_connection):
        assert patch_db_connection.__name__ == "MockedConnection"

    def test_connection_failure_raises_database_error(
        self, db_credentials_fail, patch_db_connection
    ):
        with pytest.raises(DatabaseError, match="Connection unsuccessful"):
            patch_db_connection(*db_credentials_fail)

    def test_connect_to_db_success(self, patch_db_connection):
        conn = patch_db_connection()

        assert conn.user == "user"
        assert conn.password == "pass"
        assert conn.database == "db"
        assert conn.host == "host"
        assert conn.port == 5432
