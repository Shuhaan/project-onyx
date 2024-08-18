import pytest, logging
import pandas as pd
from extract_lambda.extract import extract
from transform_lambda.utils import log_message, create_df_from_json_in_bucket


class TestLogMessage:
    @pytest.mark.parametrize(
        "level, level_name, message",
        [
            (10, "DEBUG", "This is a debug message"),
            (20, "INFO", "This is an info message"),
            (30, "WARNING", "This is a warning message"),
            (40, "ERROR", "This is an error message"),
            (50, "CRITICAL", "This is a critical message"),
        ],
        ids=[
            "Debug level",
            "Info level",
            "Warning level",
            "Error level",
            "Critical level",
        ],
    )
    def test_log_message_levels(self, caplog, level, level_name, message):
        caplog.set_level(level)
        log_message("function_name", level, message)

        assert caplog.messages == [message]
        assert level_name in caplog.text


class TestCreateDFFromJSONInBucket:
    def test_create_df_from_json_in_bucket_returns_data_frame(
        self, write_files_to_ingested_data_bucket
    ):
        ingested_data_files = write_files_to_ingested_data_bucket.list_objects(
            Bucket="test-ingested-bucket"
        )["Contents"]
        ingested_files = [bucket["Key"] for bucket in ingested_data_files]

        for file in ingested_files:
            result = create_df_from_json_in_bucket("test-ingested-bucket", file)

            if file.endswith(".json"):
                assert isinstance(result, pd.DataFrame)
            else:
                assert not result
