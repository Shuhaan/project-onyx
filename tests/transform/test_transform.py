import pytest
from transform_lambda.transform import transform


class TestTransform:
    @pytest.mark.xfail
    def test_transform_puts_files_in_processed_data_bucket(
        self, s3_client, write_files_to_ingested_data_bucket
    ):
        ingested_data_files = s3_client.list_objects(Bucket="test_ingested_bucket")[
            "Contents"
        ]
        new_files = [bucket["Key"] for bucket in ingested_data_files]

        transform("test_ingested_bucket", new_files, "test_processed_bucket")

        result_list_processed_data_bucket = s3_client.list_objects(
            Bucket="test_processed_bucket"
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
