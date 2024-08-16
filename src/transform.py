import pytest
import json
from moto import mock_aws
import boto3
import os
from dotenv import load_dotenv
import pandas as pd
from src.extract import extract_from_db_write_to_s3
from pprint import pprint


def transform(source_bucket, output_bucket):
    s3_client = boto3.client("s3")
    ingested_data_files = s3_client.list_objects(Bucket=source_bucket).get(
        "Contents", []
    )
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
            if filename.startswith(f"{key}/"):
                file_dict[key].append(filename)
                break

    for table, files in file_dict.items():
        if not files:  # Skip if no files are present for the table
            print(f"No files found for table: {table}")
            continue

        file = files[0]
        json_file = s3_client.get_object(Bucket=source_bucket, Key=file)
        json_contents = json_file["Body"].read().decode("utf-8")
        data = json.loads(json_contents).get(table, [])

        if not data:  # Skip if the JSON content does not contain expected table data
            print(f"No data found for table: {table}")
            continue

        df = pd.DataFrame(data)

        # Table-specific processing
        if table == "address":
            df = df.rename(columns={"address_id": "location_id"}).drop(
                ["created_at", "last_updated"], axis=1
            )
            output_file = "dim_location.parquet"
        elif table == "design":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_file = "dim_design.parquet"
        elif table == "currency":
            df = df.drop(["created_at", "last_updated"], axis=1).assign(
                currency_name=[
                    "British Pound Sterling",
                    "United States Dollar",
                    "Euros",
                ]
            )
            output_file = "dim_currency.parquet"
        else:
            print(f"Unknown table encountered: {table}, skipping...")
            continue

        # Save and upload the processed file
        df.to_parquet(output_file)
        s3_client.upload_file(output_file, output_bucket, output_file)
        print(f"Uploaded {output_file} to {output_bucket}")

