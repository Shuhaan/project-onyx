
import pytest
from load_utils import read_parquets_from_s3, write_df_to_warehouse
import pandas as pd


# @pytest.mark.skip
class TestReadFromS3:
    
    
    def test_function_returns_list_of_dfs(self, s3_client, s3_data_buckets):
        test_parquet1 = "tests/load/dim_counterparty.parquet"
        test_parquet2 = "tests/load/dim_currency.parquet"
        test_parquet3 = "tests/load/fact_design.parquet"
        test_time_stamp = "tests/load/time_stamp.txt"
        s3_client.upload_file(test_parquet1, "test-processed-bucket", "dim_counterparty.parquet")
        s3_client.upload_file(test_parquet2, "test-processed-bucket", "dim_currency.parquet")
        s3_client.upload_file(test_parquet3, "test-processed-bucket", "fact_design.parquet")
        s3_client.upload_file(test_time_stamp, "test-processed-bucket", "time_stamp.txt")
        dim_table_names, fact_table_names, dim_df_list, fact_df_list = read_parquets_from_s3(s3_client, "2024-08-21 10:51:0500:00", "test-processed-bucket")
        assert 'time_stamp' not in dim_table_names
        assert isinstance(dim_table_names, list)
        assert isinstance(dim_df_list, list)
        assert isinstance(dim_df_list[0], pd.DataFrame)
        assert 'time_stamp' not in fact_table_names
        assert isinstance(fact_table_names, list)
        assert isinstance(fact_df_list, list)
        assert isinstance(fact_df_list[0], pd.DataFrame)
        
  
    

@pytest.mark.skip 
class TestWriteToWarehouse:
    
    def test_function_converts_parquet_to_dataframe(self, s3_client, s3_data_buckets):
        test_parquet1 = "dim_counterparty.parquet"
        test_parquet2 = "dim_currency.parquet"
        s3_client.upload_file(test_parquet1, "test-processed-bucket", test_parquet1)
        s3_client.upload_file(test_parquet2, "test-processed-bucket", test_parquet2)
    
        read_parquet = read_parquets_from_s3(s3_client, "2024-08-21 10:51:05+00:00", "test-processed-bucket")
        result = write_df_to_warehouse(read_parquet, 'load_test' )
        # assert result == 'dim_counterparty'
        
        
    # def test_function_converts_parquet_to_dataframe(self):
    #     result = write_df_to_warehouse("dim_currency.parquet", 'load_test' )
    #     assert result == 'dim_currency'
        
    
    

