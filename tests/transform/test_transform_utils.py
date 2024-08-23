import pytest
import pandas as pd
from transform_utils import (
    list_s3_files_by_prefix,
    create_df_from_json_in_bucket,
    create_dim_date,
    process_table,
)


class TestListS3FilesByPrefix:
    @pytest.mark.parametrize(
        "bucket, prefix, expected",
        [
            (
                "test-ingested-bucket",
                "",
                [
                    "address/2024/08/19/12-00-00.json",
                    "counterparty/2024/08/19/12-00-00.json",
                    "currency/2024/08/19/12-00-00.json",
                    "department/2024/08/19/12-00-00.json",
                    "design/2024/08/19/12-00-00.json",
                    "payment/2024/08/19/12-00-00.json",
                    "payment_type/2024/08/19/12-00-00.json",
                    "purchase_order/2024/08/19/12-00-00.json",
                    "sales_order/2024/08/19/12-00-00.json",
                    "staff/2024/08/19/12-00-00.json",
                    "transaction/2024/08/19/12-00-00.json",
                ],
            ),
            ("test-ingested-bucket", "address", ["address/2024/08/19/12-00-00.json"]),
        ],
        ids=["No prefix", "Specified prefix"],
    )
    def test_list_s3_files_by_prefix(
        self, write_files_to_ingested_data_bucket, bucket, prefix, expected
    ):
        result = list_s3_files_by_prefix(bucket, prefix)

        assert result == expected


class TestCreateDFFromJSONInBucket:
    def test_create_df_from_json_in_bucket_returns_data_frame(
        self, write_files_to_ingested_data_bucket
    ):
        ingested_data_files = write_files_to_ingested_data_bucket.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        ingested_files = [bucket["Key"] for bucket in ingested_data_files]

        for file in ingested_files:
            result = create_df_from_json_in_bucket(
                "test-ingested-bucket",
                file,
                s3_client=write_files_to_ingested_data_bucket,
            )

            if file.endswith(".json"):
                assert isinstance(result, pd.DataFrame)
            else:
                assert not result


class TestCreateDimDate:
    @pytest.mark.skip
    def test_create_dim_date(self, write_files_to_ingested_data_bucket):
        result = create_dim_date("1950-01-01", "2025-12-31")

        assert isinstance(
            result, pd.DataFrame
        )  # change assertion to verify data in dataframe


class TestProcessTable:
    @pytest.mark.skip
    def test_process_table(self, write_files_to_ingested_data_bucket):
        result = process_table("df", "file", timer=0)
        expected = ("df", "output_table")

        assert result == expected
