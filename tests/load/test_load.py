import pytest, os
from load import load
from load_utils import read_parquets_from_s3, write_df_to_warehouse, log_message
from dotenv import load_dotenv
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import boto3


load_dotenv()


class TestLoad:

    @pytest.mark.xfail
    def test_func_only_loads_new_data(
        self,
        s3_client,
        util_populate_mock_s3,
        util_connect_to_mock_warehouse,
        s3_data_buckets,
        create_secrets,
    ):

        load("test-processed-bucket", s3_client)

        query = "SELECT * FROM dim_counterparty;"
        response1 = util_connect_to_mock_warehouse.run(query)
        first_response_length = len(response1)
        assert response1[-1] == [1002, "Unknown Enterprises", 2002]

        test_parquet4 = "tests/load/dim_location.parquet"
        s3_data_buckets.upload_file(
            test_parquet4, "test-processed-bucket", "dim_location.parquet"
        )

        load("test-processed-bucket", s3_client)

        response2 = util_connect_to_mock_warehouse.run(query)
        # print(first_response_length)
        # print(len(response2))

    @pytest.mark.xfail
    def test_func_reads_data_from_bucket_and_writes_correctly_to_local_db(
        self,
        s3_client,
        util_populate_mock_s3,
        s3_data_buckets,
        util_connect_to_mock_warehouse,
        create_secrets,
    ):

        # s3_client = boto3.client("s3")
        bucket = "test-processed-bucket"
        # read_parquet = ()
        try:
            last_load_file = s3_client.get_object(Bucket=bucket, Key="last_load.txt")
            last_load = last_load_file["Body"].read().decode("utf-8")
            log_message(__name__, 20, f"Load function last ran at {last_load}")
        except ClientError:
            last_load = "1900-01-01 00:00:00+0000"
            log_message(__name__, 20, "Load function running for the first time")

        try:
            # read parquet from processed data s3
            read_parquet = read_parquets_from_s3(s3_client, last_load, bucket)
            # print(read_parquet)
            log_message(__name__, 10, "Parquet file(s) read from processed data bucket")
        except ClientError as e:
            log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")

        try:
            # write new data to postrges data warehouse

            write_df_to_warehouse(read_parquet)
            log_message(__name__, 10, "Data written to data warehouse")
        except ClientError as e:
            log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")

        # create new/update last_load timestamp and put into processed data bucket
        date = datetime.now(timezone.utc)
        store_last_load = date.strftime("%Y-%m-%d %H:%M:%S%z")
        s3_client.put_object(Bucket=bucket, Key="last_load.txt", Body=store_last_load)

        query = "SELECT * FROM dim_counterparty;"
        response1 = util_connect_to_mock_warehouse.run(query)
        # print(response1)

    # def test_write_df_to_warehouse_function(self, s3_client, util_populate_mock_s3):
    #     load("test-processed-bucket", s3_client)
    #     last_load = '2024-08-20 23:10:51+0000'

    #     result = read_parquets_from_s3(s3_client, last_load, "test-processed-bucket")
    #     assert result
