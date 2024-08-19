import pytest
import pandas as pd
from transform_lambda.utils import create_df_from_json_in_bucket


class TestCreateDFFromJSONInBucket:
    @pytest.mark.xfail
    def test_create_df_from_json_in_bucket_returns_data_frame(
        self, write_files_to_ingested_data_bucket
    ):
        print(write_files_to_ingested_data_bucket.list_buckets())
        ingested_data_files = write_files_to_ingested_data_bucket.list_objects(
            Bucket="test_ingested_bucket"
        )["Contents"]
        ingested_files = [bucket["Key"] for bucket in ingested_data_files]

        for file in ingested_files:
            result = create_df_from_json_in_bucket("test_ingested_bucket", file)

            if file.endswith(".json"):
                assert isinstance(result, pd.DataFrame)
            else:
                assert not result
