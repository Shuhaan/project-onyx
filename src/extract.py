import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from connection import connect_to_db
from utils import format_response, log_message
# from pg8000.native import Connection


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True, datefmt="%d/%m/%Y %I:%M:%S %p",
)

# Create a logger instance
logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    log_message(__name__, 10, "Entered lambda_handler")
    extract_from_db_write_to_s3("onyx-totesys-ingested-data-bucket")


def extract_from_db_write_to_s3(bucket, s3_client=None):
    log_message(__name__, 10, "Entered extract function")
    if not s3_client:
        s3_client = boto3.client("s3")
    conn = None
    try:
        conn = connect_to_db()
        print(conn)
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
                query += f"WHERE last_updated > '{last_extract}'"
            query += ";"

            # add check to compare new data with old data and update if there are updates.

            # if response doesn't have modified data, don't upload file.
            response = conn.run(query)
            columns = [col["name"] for col in conn.columns]
            formatted_response = {table: format_response(columns, response)}
            extracted_json = json.dumps(formatted_response, indent=4)
            s3_key = f"{table}/{date_str}.json"
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=extracted_json)
            log_message(__name__, 20, f"{s3_key} was written to {bucket}")
        store_last_extract = date.strftime("%Y-%m-%d %H:%M:%S")
        s3_client.put_object(
            Bucket=bucket, Key="last_extract.txt", Body=store_last_extract
        )
        log_message(__name__, 20, f"Extract function completed at {store_last_extract}")


    except ClientError as e:
        log_message(__name__, 40, e.response["Error"]["Message"])

    finally:
        if conn:
            conn.close()
            log_message(__name__, 20, "DB connection closed")
