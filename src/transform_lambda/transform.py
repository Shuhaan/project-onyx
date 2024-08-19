import boto3, logging
from botocore.exceptions import ClientError
from typing import Any
from transform_utils import (
    log_message,
    create_df_from_json_in_bucket,
    create_dim_date,
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
    log_message(__name__, 10, "Entered lambda_handler")
    new_files = (
        event  # You may need to modify this to extract file names from the event
    )
    transform(
        "onyx-totesys-ingested-data-bucket",
        new_files,
        "onyx-processed-data-bucket",
        "1950-01-01",
        "2024-12-31",
    )


def transform(source_bucket: str, files: list, output_bucket: str):
    """
    Transforms JSON files from S3 and uploads the processed files back to S3,
    including generating dim_date separately.

    Args:
        source_bucket (str): The name of the S3 bucket containing the source JSON files.
        files (List[str]): List of file paths (keys) within the source bucket.
        output_bucket (str): The name of the S3 bucket to upload processed files to.
    """
    s3_client = boto3.client("s3")

    # Create the dim_date parquet
    dim_date_df = create_dim_date("1950-01-01", "2024-12-31")
    dim_date_df.to_parquet("dim_date.parquet")
    s3_client.upload_file(
        "dim_date.parquet", output_bucket, "dim_date.dim_date.parquet"
    )

    for file in files:
        table = file.split("/")[0]
        df = create_df_from_json_in_bucket(source_bucket, file)

        # Table-specific processing
        if table == "address":
            df = df.rename(columns={"address_id": "location_id"}).drop(
                ["created_at", "last_updated"], axis=1
            )
            output_file = "dim_location.parquet"

        elif table == "design":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_file = "dim_design.parquet"

        elif table == "currency":
            # Define the mapping of currency codes to currency names
            currency_mapping = {
                "GBP": "British Pound Sterling",
                "USD": "United States Dollar",
                "EUR": "Euros",
            }

            # Drop the columns and add the new currency_name column based on the currency_code
            df = df.drop(["created_at", "last_updated"], axis=1).assign(
                currency_name=df["currency_code"].map(currency_mapping)
            )
            output_file = "dim_currency.parquet"

        elif table == "counterparty":
            df = df.drop(
                [
                    "commercial_contact",
                    "delivery_contact",
                    "created_at",
                    "last_updated",
                ],
                axis=1,
            )
            output_file = "dim_counterparty.parquet"

        else:
            output_file = ""
            print(f"Unknown table encountered: {table}, skipping...")

        # Save and upload the processed file
        if output_file:
            df.to_parquet(output_file)
            s3_client.upload_file(output_file, output_bucket, output_file)
            print(f"Uploaded {output_file} to {output_bucket}")
