import boto3, logging
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Any
from load_utils import (
    log_message,
    read_parquets_from_s3,
    write_df_to_warehouse,
    get_secret,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
    datefmt="%d/%m/%Y %I:%M:%S %p",
)

# Create a logger instance
logger = logging.getLogger(__name__)

def lambda_handler(event: dict, context: Any):
    """
    AWS Lambda function entry point.

    This function processes an event to load new files, transforms the data from
    parquet files
    and uploads the processed files to it's associated data warhehouse table.

    :param event: The event data passed to the Lambda function (as a dictionary).
    :param context: The runtime information of the Lambda function (e.g., function name, version).
    """
    log_message(__name__, 10, "Entered load_lambda_handler")

    source_bucket = "onyx-processed-data-bucket"

    s3_client = boto3.client("s3")

    load(source_bucket, s3_client=s3_client)


def load(bucket="onyx-processed-data-bucket", s3_client=None):
    """_summary_

    Args:
        bucket (str, optional): _description_. Defaults to "onyx-processed-data-bucket".
        s3_client (_type_, optional): _description_. Defaults to None.
    """
    log_message(__name__, 10, "Entered load function")
    if not s3_client:
        s3_client = boto3.client("s3")

    try:
        last_load_file = s3_client.get_object(Bucket=bucket, Key="last_load.txt")
        last_load = last_load_file["Body"].read().decode("utf-8")
        log_message(__name__, 20, f"Load function last ran at {last_load}")
    except s3_client.exceptions.NoSuchKey:
        last_load = "1900-01-01 00:00:00+0000"
        log_message(__name__, 20, "Load function running for the first time")

    try:
        # read parquet from processed data s3
        read_parquet = read_parquets_from_s3(s3_client, last_load, bucket)
        log_message(__name__, 10, "Parquet file(s) read from processed data bucket")
    except ClientError as e:
        log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")

    try:
        # write new data to postrges data warehouse
        write_df_to_warehouse(read_parquet, engine_string=None)
        log_message(__name__, 10, "Data written to data warehouse")
        # create new/update last_load timestamp and put into processed data bucket
        date = datetime.now(timezone.utc)
        store_last_load = date.strftime("%Y-%m-%d %H:%M:%S%z")
        s3_client.put_object(Bucket=bucket, Key="last_load.txt", Body=store_last_load)
    except ClientError as e:
        log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")


