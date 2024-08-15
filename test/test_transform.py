import pytest
import json
from moto import mock_aws
import boto3




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


# class TestTransform:
#     def test_extract_writes_all_tables_to_s3_as_directories(
#         self, s3_client, s3_ingested_data_bucket, create_secrets
#     ):

#         extract_from_db_write_to_s3("bucket", s3_client)
#         result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
#         result = [bucket["Key"] for bucket in result_list_bucket]
#         expected = [
#             "address",
#             "counterparty",
#             "currency",
#             "department",
#             "design",
#             "last_extract.txt",
#             "payment",
#             "payment_type",
#             "purchase_order",
#             "sales_order",
#             "staff",
#             "transaction",
#         ]
#         for folder, table in zip(result, expected):
#             assert table in folder

#     def test_extract_writes_jsons_into_s3_with_correct_data_from_db(
#         self, s3_client, s3_ingested_data_bucket, create_secrets
#     ):

#         extract_from_db_write_to_s3("bucket", s3_client)
#         result_list_bucket = s3_client.list_objects(Bucket="bucket")["Contents"]
#         result = [bucket["Key"] for bucket in result_list_bucket]
#         for key in result:
#             if ".txt" not in key:
#                 json_file = s3_client.get_object(Bucket="bucket", Key=key)
#                 json_contents = json_file["Body"].read().decode("utf-8")
#                 content = json.loads(json_contents)
#                 for folder in content:
#                     assert content[folder][0]["created_at"]
