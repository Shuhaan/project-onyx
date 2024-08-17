import pytest
from moto import mock_aws
from dotenv import load_dotenv
from extract_lambda.extract import extract
from transform_lambda.transform import transform


class TestTransform:
    @pytest.mark.skip
    def test_transform_puts_files_in_processed_data_bucket(
        self, s3_client, write_files_to_ingested_date_bucket
    ):
        ingested_data_files = s3_client.list_objects(
            Bucket="onyx-totesys-ingested-data-bucket"
        )["Contents"]
        new_files = [bucket["Key"] for bucket in ingested_data_files]

        transform(
            "onyx-totesys-ingested-data-bucket", new_files, "onyx-processed-data-bucket"
        )

        result_list_processed_data_bucket = s3_client.list_objects(
            Bucket="onyx-processed-data-bucket"
        )["Contents"]
        result = [bucket["Key"] for bucket in result_list_processed_data_bucket]
        print(result)

        expected = [
            "dim_staff.parquet",
            "dim_location.parquet",
            "dim_design.parquet",
            "dim_date.parquet",
            "dim_currency.parquet",
            "dim_counterparty.parquet",
            "fact_sales_order.parquet",
        ]

        for table in expected:
            assert any([folder.startswith(table) for folder in result])
