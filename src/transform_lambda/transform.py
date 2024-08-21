import boto3, logging
from botocore.exceptions import ClientError
from typing import Any
from transform_utils import (
    get_bucket_contents,
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
    log_message(__name__, 10, "Entered transform_lambda_handler")
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    new_file = event["Records"][0]["s3"]["object"]["key"]

    transform(source_bucket, new_file, "onyx-processed-data-bucket")


def transform(source_bucket: str, file: str, output_bucket: str):
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
    output_bucket_contents = get_bucket_contents(output_bucket)

    # Create the dim_date parquet if it does not exist
    if "dim_date.dim_date.parquet" not in output_bucket_contents:
        dim_date_df = create_dim_date("1970-01-01", "2030-12-31")
        dim_date_df.to_parquet("dim_date.parquet")
        s3_client.upload_file(
            "dim_date.parquet", output_bucket, "dim_date.dim_date.parquet"
        )

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
        # to do - include error handling code for currencies not in above dict

        # Drop the columns and add the new currency_name column based on the currency_code
        df = df.drop(["created_at", "last_updated"], axis=1).assign(
            currency_name=df["currency_code"].map(currency_mapping)
        )
        output_file = "dim_currency.parquet"

    elif table == "counterparty":  # combine counterparty with address table
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
        log_message(__name__, 20, f"Unknown table encountered: {table}, skipping...")

    # Save and upload the processed file
    if output_file.endswith(".parquet"):
        df.to_parquet(output_file)
        s3_client.upload_file(output_file, output_bucket, output_file)
        log_message(__name__, 20, f"Uploaded {output_file} to {output_bucket}")


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
