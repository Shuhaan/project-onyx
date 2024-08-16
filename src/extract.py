from src.connection import connect_to_db
from src.utils import format_response, log_message
from botocore.exceptions import ClientError
import boto3
from datetime import datetime
import json


# Arif's logging function to be applied


def extract_from_db_write_to_s3(bucket, s3_client=None):
    if not s3_client:
        s3_client = boto3.client("s3")

    try:
        global conn
        conn = connect_to_db()

        date = datetime.now()
        date_str = date.strftime("%Y/%m/%d/%H-%M")

        try:
            last_extract_file = s3_client.get_object(
                Bucket=bucket, Key="last_extract.txt"
            )
            last_extract = last_extract_file["Body"].read().decode("utf-8")
        except:
            last_extract = None

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
                query += f"WHERE last_updated > {last_extract}"
            query += ";"

            response = conn.run(query)
            columns = [col["name"] for col in conn.columns]
            formatted_response = {table: format_response(columns, response)}
            extracted_json = json.dumps(formatted_response, indent=4)
            s3_key = f"{table}/{date_str}.json"
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=extracted_json)

        store_last_extract = date.strftime("%Y-%m-%d %H:%M:%S")
        s3_client.put_object(
            Bucket=bucket, Key="last_extract.txt", Body=store_last_extract
        )

        # add check to compare new data with old data and update if there are updates.

    except ClientError as e:
        name = __name__
        log_message(name, "40", e.response["Error"]["Message"])
    finally:
        conn.close()
