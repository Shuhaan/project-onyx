import pytest, json
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

        extract("test-ingested-bucket", s3_client)
        result_list_bucket = s3_client.list_objects(Bucket="test-ingested-bucket")[
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

    def extract_test_data(self, client):
        """
        Utility test for extracting data from mock bucket

        :param client (s3_client): required to give access to the mocked client.

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


    @patch("extract_lambda.extract.connect_to_db", return_value=MockedConnection())
    def test_extract_writes_jsons_into_s3_with_correct_structure_from_db(
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        create_secrets,
    ):
        all_tables = self.extract_test_data(s3_client)
        for dict_table in all_tables:
            print(dict_table)
            for table_data in dict_table.values():
                print (table_data)
                assert table_data[0]["last_updated"]

    def test_extract_writes_jsons_into_s3_with_correct_data_type_from_db(
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        create_secrets,
    ):

        all_tables = self.extract_test_data(s3_client)
        for dict_table in all_tables:
            print(dict_table)
            for table_data in dict_table.values():
                value = table_data[0]["last_updated"]
                date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                assert isinstance(date, datetime)

                
    def test_mocked_connection_patch_working(
        self,
        patch_db_connection,
        aws_credentials,
        s3_client,
        s3_data_buckets
    ):
        def verify_extract(mock_data):
            """
            Utility test for verifying data from mock bucket.
            Asserts if data in bucket is consistent with mock_data.

            :param mock_data: required to give access to the mocked client.

            :return: None
            """       
            extract("test-ingested-bucket", s3_client)
            result_list_bucket = s3_client.list_objects(Bucket="test-ingested-bucket")[
                "Contents"
            ]
            result = [bucket["Key"] for bucket in result_list_bucket]
            
            for key in result:
                if 'address' in key:
                    json_file = s3_client.get_object(Bucket="test-ingested-bucket", Key=key)
                    json_contents = json_file["Body"].read().decode("utf-8")
                    assert json.loads(json_contents)["address"][0]["meaningful_data"] == mock_data
        
        verify_extract("old_data1")
        verify_extract("new_data1")
