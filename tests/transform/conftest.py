import pytest
from extract_lambda.extract import extract


@pytest.fixture(scope="function")
def write_files_to_ingested_data_bucket(create_secrets, s3_data_buckets):
    extract("test-ingested-bucket")
