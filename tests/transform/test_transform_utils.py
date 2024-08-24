import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import BytesIO
from transform_utils import (
    list_s3_files_by_prefix,
    create_df_from_json_in_bucket,
    create_dim_date,
    process_table,
    combine_parquet_from_s3,
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
    def test_create_dim_date(self):
        # Given start and end dates
        start_date = "2020-01-01"
        end_date = "2020-01-05"

        # When creating the dim_date DataFrame
        result = create_dim_date(start_date, end_date)

        # Then the result should be a DataFrame
        assert isinstance(result, pd.DataFrame)

        # And it should have the correct length
        assert len(result) == 5  # 5 days including start and end

        # And it should have the expected columns
        expected_columns = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        assert list(result.columns) == expected_columns

        # And the first row should match the start date
        assert result.iloc[0]["date_id"] == "2020-01-01"

        # And the last row should match the end date
        assert result.iloc[-1]["date_id"] == "2020-01-05"

        # Additional checks for specific values
        assert result.iloc[0]["day_of_week"] == 3  # Wednesday
        assert result.iloc[0]["month_name"] == "January"
        assert result.iloc[0]["quarter"] == 1


class TestProcessTable:
    @patch("transform_utils.combine_parquet_from_s3")
    def test_process_table(self, mock_combine_parquet):
        # Given a sample DataFrame and file name
        df = pd.DataFrame(
            {
                "address_id": [1, 2],
                "created_at": ["2020-01-01 12:00:00", "2020-01-02 13:00:00"],
                "last_updated": ["2020-01-03 14:00:00", "2020-01-04 15:00:00"],
            }
        )
        file = "address/some_file.parquet"
        bucket = "test-bucket"

        # When processing the table
        result_df, output_table = process_table(df, file, bucket)

        # Then the result DataFrame should be correct
        expected_df = pd.DataFrame({"location_id": [1, 2]})
        pd.testing.assert_frame_equal(result_df, expected_df)

        # And the output table should be 'dim_location'
        assert output_table == "dim_location"

    def test_process_unknown_table(self):
        # Given a DataFrame and an unknown file name
        df = pd.DataFrame({"unknown_column": [1, 2]})
        file = "unknown_table/some_file.parquet"
        bucket = "test-bucket"

        # When processing the table
        result = process_table(df, file, bucket)

        # Then the result should be None or the function should skip processing
        assert result is None


class TestCombineParquetFromS3:
    @patch("transform_utils.boto3.client")
    @patch("transform_utils.list_s3_files_by_prefix")
    def test_combine_parquet_from_s3(self, mock_list_s3_files, mock_boto3_client):
        # Given a list of S3 files
        mock_list_s3_files.return_value = [
            "directory/file1.parquet",
            "directory/file2.parquet",
        ]

        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Mock S3 response for each file
        mock_s3_client.get_object.side_effect = [
            {"Body": BytesIO(pd.DataFrame({"col1": [1]}).to_parquet())},
            {"Body": BytesIO(pd.DataFrame({"col1": [2]}).to_parquet())},
        ]

        # When combining parquet files from S3
        result = combine_parquet_from_s3("test-bucket", "directory")

        # Then the result should be a DataFrame with combined data
        expected_df = pd.DataFrame({"col1": [1, 2]})
        pd.testing.assert_frame_equal(result, expected_df)
