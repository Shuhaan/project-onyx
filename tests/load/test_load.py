import pytest
from extract import extract
# from load import load#, read_parquets_from_s3
# print(load.path)
# from load_utils import read_parquets_from_s3, write_df_to_warehouse, get_secret



class TestLoad:

    def test_func_only_loads_new_data(self):
        pass

    # def test_timestamp_condition_passing_in_read_parquet_function(self, s3_client, util_populate_mock_s3):
    #     load("test-processed-bucket", s3_client)
    #     last_load = "2024-08-30 23:10:51+0000"
    #     assert read_parquets_from_s3(s3_client, last_load, "test-processed-bucket")