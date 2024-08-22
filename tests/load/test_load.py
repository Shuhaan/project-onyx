import pytest
from load import load 
from load_utils import read_parquets_from_s3

class TestLoad:

    def test_func_only_loads_new_data(self, s3_client, util_populate_mock_s3, \
                                          util_connect_to_mock_warehouse):
        last_load = "2024-08-21 23:10:51+0000"
        read_parquet = read_parquets_from_s3(s3_client, 
                                             last_load, 
                                             "test-processed-bucket")
        write_df_to_warehouse(read_parquet, engine_string=os.getenv("TEST-ENGINE"))
        
        query = "SELECT * FROM dim_counterparty;"
        response = util_connect_to_mock_warehouse.run(query)
        assert response == [
            [1001, 'Anonymous Co.', 2001], 
            [1002, 'Unknown Enterprises', 2002]
        ]
        pass

    def test_write_df_to_warehouse_function(self, s3_client, util_populate_mock_s3):
        load("test-processed-bucket", s3_client)
        last_load = '2024-08-20 23:10:51+0000'
        
        result = read_parquets_from_s3(s3_client, last_load, "test-processed-bucket")
        assert result
