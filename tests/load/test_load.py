import pytest
from load import load
from load_utils import read_parquets_from_s3


class TestLoad:

    def test_func_only_loads_new_data(self):
        pass

    def test_timestamp_condition_passing_in_read_parquet_function(self,s3_client, util_populate_mock_s3):
        load("test-processed-bucket", s3_client)
        assert read_parquets_from_s3("test-processed-bucket")