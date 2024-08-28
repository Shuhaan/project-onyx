import boto3, logging
from botocore.exceptions import ClientError
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from typing import Any
from load_utils import (
    log_message,
    read_parquets_from_s3,
    write_df_to_warehouse,
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
    parquet files, and uploads the processed files to its associated data warehouse table.

    :param event: The event data passed to the Lambda function (as a dictionary).
    :param context: The runtime information of the Lambda function (e.g., function name, version).
    :return: A dictionary with the status of the operation.
    """
    log_message(__name__, 10, "Entered lambda_handler")

    try:
        # Extract source bucket from event or use default
        source_bucket = event.get("source_bucket", "onyx-processed-data-bucket")

        # Initialize the S3 client
        s3_client = boto3.client("s3")

        # Call the load function
        load(source_bucket, s3_client=s3_client)

        # Log completion and return success response
        log_message(__name__, 20, "Load process completed successfully")
        return {"statusCode": 200, "body": "Load process completed successfully"}

    except Exception as e:
        # Log the error and return failure response
        log_message(__name__, 40, f"Error in lambda_handler: {str(e)}")
        return {"statusCode": 500, "body": f"Error in lambda_handler: {str(e)}"}


def load(bucket="onyx-processed-data-bucket", s3_client=None):
    """
    Load function to process parquet files from S3 and write the data to a data warehouse.

    Args:
        bucket (str, optional): The S3 bucket name. Defaults to "onyx-processed-data-bucket".
        s3_client (boto3.client, optional): The S3 client instance. Defaults to None.
    """
    log_message(__name__, 10, "Entered load function")

    # Initialize the S3 client if not provided
    if not s3_client:
        s3_client = boto3.client("s3")

    # Retrieve the last load timestamp from S3 or set default if not present
    try:
        last_load_file = s3_client.get_object(Bucket=bucket, Key="last_load.txt")
        last_load = last_load_file["Body"].read().decode("utf-8")
        log_message(__name__, 20, f"Load function last ran at {last_load}")
    except s3_client.exceptions.NoSuchKey:
        last_load = "1900-01-01 00:00:00+0000"
        log_message(__name__, 20, "Load function running for the first time")

    # Write the DataFrame(s) to the data warehouse
    try:
        tables = [
            "dim_staff",
            "dim_location",
            "dim_counterparty",
            "dim_currency",
            "dim_design",
            "dim_transaction",
            "dim_payment_type",
            "fact_sales_order",
            "fact_purchase_order",
            "dim_date",
            "fact_payment",
        ]
        for table in tables:

            df_list = read_parquets_from_s3(s3_client, table, last_load, bucket)
            log_message(__name__, 20, f"Parquet file(s) for {table} read from processed data bucket")
            write_df_to_warehouse(
                df_list, table, engine_string=None
            )  # Pass engine_string if required
            log_message(__name__, 20, f"Data written to {table} in data warehouse")

    except SQLAlchemyError as e:  # Handle SQLAlchemy errors specifically
        log_message(__name__, 40, f"Warehouse write error: {str(e)}")
        raise e

    except ClientError as e:
        log_message(
            __name__,
            40,
            f"Error: {e.response['Error']['Message']}",
        )
        raise e

    # Update the last load timestamp in S3
    try:
        date = datetime.now(timezone.utc)
        store_last_load = date.strftime("%Y-%m-%d %H:%M:%S%z")
        s3_client.put_object(Bucket=bucket, Key="last_load.txt", Body=store_last_load)
        log_message(__name__, 20, f"Updated last load timestamp to {store_last_load}")

    except ClientError as e:
        log_message(
            __name__,
            40,
            f"Error updating last load timestamp: {e.response['Error']['Message']}",
        )
