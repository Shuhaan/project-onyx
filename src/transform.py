import pytest
import json
from moto import mock_aws
import boto3
import os
from dotenv import load_dotenv
from src.extract import extract_from_db_write_to_s3
from pprint import pprint


def transform(source_bucket, output_bucket):
    s3_client = boto3.client("s3")
    ingested_data_files = s3_client.list_objects(Bucket=source_bucket)["Contents"]
    ingested_data_files_names = [file_data["Key"] for file_data in ingested_data_files]

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
    ]

    file_dict = {name: [] for name in expected}

    for filename in ingested_data_files_names:
        for key in file_dict.keys():
            if filename.startswith(key):
                file_dict[key].append(filename)
                break

    print(file_dict)

    # for key in ingested_data_files_names:
    #     if not key.endswith(".txt"):  # Filter out .txt files
    #         # Get the file from the source bucket
    #         json_file = s3_client.get_object(Bucket=source_bucket, Key=key)
    #         json_contents = json_file["Body"].read().decode("utf-8")
    #         content = json.loads(json_contents)

    #         pprint(content)
