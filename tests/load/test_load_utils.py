
import pytest
from load_utils import read_parquets_from_s3, write_df_to_warehouse
import pandas as pd


# @pytest.mark.skip
class TestReadFromS3:
    
    
    def test_function_returns_list_of_dfs(self, s3_client, s3_data_buckets):
        test_parquet1 = "dim_counterparty.parquet"
        test_parquet2 = "dim_currency.parquet"
        s3_client.upload_file(test_parquet1, "test-processed-bucket", test_parquet1)
        s3_client.upload_file(test_parquet2, "test-processed-bucket", test_parquet2)
        df_list = read_parquets_from_s3(s3_client, "test-processed-bucket")
        assert isinstance(df_list[0], str)
        assert isinstance(df_list[1], list)
        assert isinstance(df_list[1][0], pd.DataFrame)
        
  
    

# @pytest.mark.skip 
class TestWriteToWarehouse:
    
    def test_function_converts_parquet_to_dataframe(self, s3_client, s3_data_buckets):
        test_parquet1 = "dim_counterparty.parquet"
        test_parquet2 = "dim_currency.parquet"
        s3_client.upload_file(test_parquet1, "test-processed-bucket", test_parquet1)
        s3_client.upload_file(test_parquet2, "test-processed-bucket", test_parquet2)
    
        read_parquet = read_parquets_from_s3(s3_client, "test-processed-bucket")
        result = write_df_to_warehouse(read_parquet, 'load_test' )
        assert result == 'dim_counterparty'
        
        
    # def test_function_converts_parquet_to_dataframe(self):
    #     result = write_df_to_warehouse("dim_currency.parquet", 'load_test' )
    #     assert result == 'dim_currency'
        
    
    

