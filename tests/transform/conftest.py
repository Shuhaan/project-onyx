import pytest, json
from datetime import datetime


@pytest.fixture(scope="function")
def write_files_to_ingested_data_bucket(s3_data_buckets):
    # Configuration
    input_file_path = "tests/transform/mock_ingested_data.json"
    bucket_name = "test-ingested-bucket"

    # Read the data from the input JSON file
    with open(input_file_path, "r") as file:
        data = json.load(file)

    # Upload each table's data to S3
    for table_name, table_data in data.items():
        # Create the S3 key (filename) based on the table name and current date/time
        s3_key = f"{table_name}/2024/08/19/12-00-00.json"

        # Convert the table data to JSON string
        json_data = json.dumps({table_name: table_data}, indent=4)

        # Upload the data to S3
        s3_data_buckets.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json",
        )
    return s3_data_buckets
