import pytest, logging, os, boto3
import pandas as pd
from moto import mock_aws
from dotenv import load_dotenv
from extract_lambda.extract import extract
from transform_lambda.utils import log_message, create_df_from_json


class TestLogMessage:
    def test_log_message(self, caplog):
        caplog.set_level(logging.INFO)
        result = log_message("function_name", 30, "This is a warning")
        expected = ["This is a warning"]
        assert caplog.messages == expected
        assert "WARNING" in caplog.text


class TestCreateDFFromJSON:
    def test_create_df_from_json_returns_data_frame(
        self,
        aws_credentials,
        s3_client,
        s3_data_buckets,
        write_files_to_ingested_data_bucket,
    ):
        ingested_data_files = s3_client.list_objects(Bucket="test-ingested-bucket")[
            "Contents"
        ]
        ingested_files = [bucket["Key"] for bucket in ingested_data_files]

        for file in ingested_files:
            result = create_df_from_json("test-ingested-bucket", file)

            if file.endswith(".json"):
                assert isinstance(result, pd.DataFrame)
            else:
                assert not result
