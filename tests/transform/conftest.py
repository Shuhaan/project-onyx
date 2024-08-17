import pytest
from extract_lambda.extract import extract


@pytest.fixture()
def write_files_to_ingested_date_bucket(create_secrets, s3_data_buckets):
    extract("onyx-totesys-ingested-data-bucket")
