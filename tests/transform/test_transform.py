import pytest
from transform import transform


class TestTransform:
    def test_transform_puts_files_in_processed_data_bucket(
        self, write_files_to_ingested_data_bucket
    ):
        ingested_data_files = write_files_to_ingested_data_bucket.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        new_files = [bucket["Key"] for bucket in ingested_data_files]

        for new_file in new_files:
            transform(
                "test-ingested-bucket", new_file, "test-processed-bucket", timer=0
            )

        result_list_processed_data_bucket = (
            write_files_to_ingested_data_bucket.list_objects(
                Bucket="test-processed-bucket"
            )["Contents"]
        )
        result = [bucket["Key"] for bucket in result_list_processed_data_bucket]

        expected = [
            "dim_staff",
            "dim_location",
            "dim_design",
            "dim_date",
            "dim_currency",
            "dim_counterparty",
            "fact_sales_order",
        ]

        for table in expected:
            assert any([folder.startswith(table) for folder in result])
