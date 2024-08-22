import boto3, logging
from botocore.exceptions import ClientError
from typing import Any
from datetime import datetime
import pandas as pd
from transform_utils import (
    list_s3_files_by_prefix,
    log_message,
    create_df_from_json_in_bucket,
    create_dim_date,
    process_table,
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

    This function processes an event to extract new files, transforms the data from JSON files
    and uploads the processed files to a specified S3 bucket. It also generates a 'dim_date'
    parquet file and uploads it to S3.

    :param event: The event data passed to the Lambda function (as a dictionary).
    :param context: The runtime information of the Lambda function (e.g., function name, version).
    """
    log_message(__name__, 10, "Entered transform_lambda_handler")
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    new_file = event["Records"][0]["s3"]["object"]["key"]

    transform(source_bucket, new_file, "onyx-processed-data-bucket")


def transform(source_bucket: str, file: str, output_bucket: str, timer: int = 120):
    """
    Transforms JSON files from S3 and uploads the processed files back to S3,
    including generating dim_date separately.

    Args:
        source_bucket (str): The name of the S3 bucket containing the source JSON files.
        file (str): str of file path (key) within the source bucket.
        output_bucket (str): The name of the S3 bucket to upload processed files to.
    """
    log_message(__name__, 20, "Transform started")

    s3_client = boto3.client("s3")

    output_bucket_contents = list_s3_files_by_prefix(output_bucket)
    date = datetime.now()
    date_str = date.strftime("%Y/%m/%d/%H-%M")

    # Create the dim_date parquet if it does not exist
    if not any([file.startswith("dim_date") for file in output_bucket_contents]):
        dim_date_df = create_dim_date("1970-01-01", "2030-12-31")
        dim_date_df.to_parquet("/tmp/dim_date.parquet")
        s3_client.upload_file(
            "/tmp/dim_date.parquet", output_bucket, f"dim_date/{date_str}.parquet"
        )

    table = file.split("/")[0]
    df = create_df_from_json_in_bucket(source_bucket, file)
    df, output_table = process_table(df, table, output_bucket, timer=timer)
    # print(output_table)
    # print(df)

    # Save and upload the processed file
    if output_table:
        s3_key = f"{output_table}/{date_str}.parquet"
        df.to_parquet(f"/tmp/{output_table}.parquet")
        s3_client.upload_file(f"/tmp/{output_table}.parquet", output_bucket, s3_key)
        log_message(__name__, 20, f"Uploaded {output_table} to {output_bucket}")


# "address",
# "design",
# "transaction",
# "sales_order",
# "counterparty",
# "payment",
# "staff",
# "purchase_order",
# "payment_type",
# "currency",
# "department"
