import json, os, boto3, logging
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Any
from extract_lambda.utils import format_response, log_message
from extract_lambda.connection import connect_to_db


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

    This function is triggered by an event and context, extracts data from a database,
    and stores the data in an S3 bucket. The bucket name is retrieved from environment variables.

    :param event: The event data passed to the Lambda function (as a dictionary).
    :param context: The runtime information of the Lambda function (e.g., function name, version).
    """
    log_message(__name__, 10, "Entered lambda_handler")
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    extract(bucket_name)


def extract(bucket: str, s3_client=None):
    """
    Extracts data from a database and stores it in an S3 bucket.

    Connects to a database, retrieves data from specified tables, formats the data,
    and uploads it to an S3 bucket. If a last extract timestamp is found, only new
    or updated records are retrieved. Updates the timestamp of the last extraction in S3.

    :param bucket: The name of the S3 bucket where data will be stored.
    :param s3_client: Optional S3 client to use for interactions with S3. If not provided, a new client is created.

    :raises ClientError: If there is an issue with S3 operations or if the specified bucket or object does not exist.
    """
    log_message(__name__, 10, "Entered extract function")
    if not s3_client:
        s3_client = boto3.client("s3")
    conn = None

    try:
        conn = connect_to_db()
        log_message(__name__, 20, "Connection to DB made")

        date = datetime.now()
        date_str = date.strftime("%Y/%m/%d/%H-%M")

        try:
            last_extract_file = s3_client.get_object(
                Bucket=bucket, Key="last_extract.txt"
            )
            last_extract = last_extract_file["Body"].read().decode("utf-8")
            log_message(__name__, 20, f"Extract function last ran at {last_extract}")
        except s3_client.exceptions.NoSuchKey:
            last_extract = None
            log_message(__name__, 20, "Extract function running for the first time")

        totesys_table_list = [
            "address",
            "design",
            "transaction",
            "sales_order",
            "counterparty",
            "payment",
            "staff",
            "purchase_order",
            "payment_type",
            "currency",
            "department",
        ]
        for table in totesys_table_list:
            query = f"SELECT * FROM {table} "
            if last_extract:
                # Add check to compare new data with old data and update if there are updates.
                query += f"WHERE last_updated > '{last_extract}'"
            query += ";"

            response = conn.run(query)
            # print(response)
            # If response doesn't have modified data, don't upload file.
            if len(response):
                columns = [col["name"] for col in conn.columns]
                formatted_response = {table: format_response(columns, response)}
                extracted_json = json.dumps(formatted_response, indent=4)
                s3_key = f"{table}/{date_str}.json"
                s3_client.put_object(Bucket=bucket, Key=s3_key, Body=extracted_json)
                
                log_message(__name__, 20, f"{s3_key} was written to {bucket}")

                # # compile all tables to add to updated-data.json
                # if not last_extract:
                #     compiled_data = formatted_response
                    
                # # if new data found, update the updated-data.json
                # if "WHERE" in query:

                #     pass

        # # keep a json file to add updated data to.
        # if not last_extract:
        #     updated_data_key = "updated-data.json"
        #     s3_client.put_object(Bucket=bucket, Key=updated_data_key, Body=extracted_json)
                        
        store_last_extract = date.strftime("%Y-%m-%d %H:%M:%S")
        s3_client.put_object(
            Bucket=bucket, Key="last_extract.txt", Body=store_last_extract
        )

    except ClientError as e:
        log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")

    finally:
        if conn:
            # print to check if patched or real connection used
            print(s3_client, "s3_client that was used")
            conn.close()
            log_message(__name__, 20, "DB connection closed")
