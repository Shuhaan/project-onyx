import pytest
from transform import transform


class TestTransform:
    @pytest.mark.xfail
    def test_transform_puts_files_in_processed_data_bucket(
        self, write_files_to_ingested_data_bucket
    ):
        ingested_data_files = write_files_to_ingested_data_bucket.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        new_files = [bucket["Key"] for bucket in ingested_data_files]

        for new_file in new_files:
            transform("test-ingested-bucket", new_file, "test-processed-bucket")

        result_list_processed_data_bucket = (
            write_files_to_ingested_data_bucket.list_objects(
                Bucket="test-processed-bucket"
            )["Contents"]
        )
        result = [bucket["Key"] for bucket in result_list_processed_data_bucket]
        print("List of files uploaded to mock processed bucket", result)

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
