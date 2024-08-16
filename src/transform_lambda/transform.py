import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from transform_lambda.utils import log_message, create_df_from_json


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
    datefmt="%d/%m/%Y %I:%M:%S %p",
)

# Create a logger instance
logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    log_message(__name__, 10, "Entered lambda_handler")
    transform("onyx-totesys-ingested-data-bucket", event, "onyx-processed-data-bucket")


def transform(source_bucket, file_name, output_bucket):
    s3_client = boto3.client("s3")
    table = file_name.split("/")[0]
    df = create_df_from_json(source_bucket, file_name)

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
        df = df.drop(["created_at", "last_updated"], axis=1).assign(
            currency_name=[
                "British Pound Sterling",
                "United States Dollar",
                "Euros",
            ]
        )

    elif table == "counterparty":
        df = df.drop(
            ["commercial_contact", "delivery_contact", "created_at", "last_updated"],
            axis=1,
        )
        output_file = "dim_counterparty.parquet"

    else:
        output_file = ""
        print(f"Unknown table encountered: {table}, skipping...")

    # Save and upload the processed file_name
    if output_file:
        df.to_parquet(output_file)
        s3_client.upload_file(output_file, output_bucket, output_file)
        print(f"Uploaded {output_file} to {output_bucket}")
