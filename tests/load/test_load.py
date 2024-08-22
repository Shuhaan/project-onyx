import pytest, os
from load import load 
from load_utils import read_parquets_from_s3, write_df_to_warehouse
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
class TestLoad:

    def test_func_only_loads_new_data(self, s3_client, util_populate_mock_s3, \
                                          util_connect_to_mock_warehouse, s3_data_buckets,
                                          create_secrets):
        
        load("test-processed-bucket", s3_client)
        
        query = "SELECT * FROM dim_counterparty;"
        response1 = util_connect_to_mock_warehouse.run(query)
        first_response_length = len(response1)
        assert response1[-1] == [1002, 'Unknown Enterprises', 2002]
        
        test_parquet4 = "tests/load/dim_location.parquet"
        s3_data_buckets.upload_file(test_parquet4,
                              "test-processed-bucket", "dim_location.parquet")
        
        load("test-processed-bucket", s3_client)
        
        response2 = util_connect_to_mock_warehouse.run(query)
        print(first_response_length)
        print(len(response2))
      




    # def test_write_df_to_warehouse_function(self, s3_client, util_populate_mock_s3):
    #     load("test-processed-bucket", s3_client)
    #     last_load = '2024-08-20 23:10:51+0000'
        
    #     result = read_parquets_from_s3(s3_client, last_load, "test-processed-bucket")
    #     assert result
