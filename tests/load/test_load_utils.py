
import pytest, os
from load_utils import read_parquets_from_s3, write_df_to_warehouse, get_secret
import pandas as pd
from datetime import datetime, tzinfo
from dotenv import load_dotenv

load_dotenv()


# @pytest.mark.skip
class TestReadFromS3:
    
    def test_function_returns_list_of_dfs(self,s3_client, util_populate_mock_s3):
        dim_table_names, fact_table_names, dim_df_list, fact_df_list \
        = read_parquets_from_s3(s3_client, "2024-08-21 10:51:05+00:00", "test-processed-bucket")
        assert 'time_stamp' not in dim_table_names
        assert isinstance(dim_table_names, list)
        assert isinstance(dim_df_list, list)
        assert isinstance(dim_df_list[0], pd.DataFrame)
        assert 'time_stamp' not in fact_table_names
        assert isinstance(fact_table_names, list)
        assert isinstance(fact_df_list, list)
        assert isinstance(fact_df_list[0], pd.DataFrame)
        
# @pytest.mark.skip 
class TestWriteToWarehouse:
    
    def test_function_writes_to_warehouse(self,s3_client, util_populate_mock_s3, \
                                          util_connect_to_mock_warehouse):
        read_parquet = read_parquets_from_s3(s3_client, 
                                             "2024-08-21 10:51:05+00:00", 
                                             "test-processed-bucket")
        # bucket_contents = s3_client.list_objects(Bucket="test-processed-bucket")["Contents"]
        # last_mod = bucket_contents[0]['LastModified']
        # now=None
        # print(now > last_mod,\
        #     "<<<<<< compare with now")
        # read_parquet = read_parquets_from_s3(s3_client, 
        #                                      f"{now}+00:00", 
        #                                      "test-processed-bucket")
        write_df_to_warehouse(read_parquet, os.getenv("TEST-ENGINE"))
        
        query = "SELECT * FROM dim_counterparty LIMIT 2;"
        response = util_connect_to_mock_warehouse.run(query)
        assert response == [
            [1001, 'Anonymous Co.', 2001], 
            [1002, 'Unknown Enterprises', 2002]
        ]