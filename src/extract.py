from src.connection import connect_to_db
from utilities.utils import format_response, log_message
from botocore.exceptions import ClientError
import logging
import boto3
from datetime import datetime
from pprint import pprint
import json


# logger = logging.getLogger("OnyxLogger")
# logger.setLevel(logging.INFO)

most_recent_extract = ""


# def extract_from_db(bucket_name, s3_resource=boto3.resource('s3')):
def extract_from_db_write_to_s3(s3_client, bucket):
    # bucket = s3_resource.Bucket(bucket_name)
    try:
        conn = connect_to_db()

        date = datetime.now()
        date_str = date.strftime("%Y/%m/%d/%H-%M")

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
            query = f"""SELECT * FROM {table}
                        LIMIT 2;
                        """
            # logic required to check timestamps for updates

            response = conn.run(query)
            columns = [col["name"] for col in conn.columns]
            formatted_response = {table: format_response(columns, response)}
            extracted_json = json.dumps(formatted_response, indent=4)
            # print(extracted_json)

            s3_key = f"{table}/{date_str}.json"

            most_recent_extract = s3_key

            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=extracted_json
            )

            # response = bucket.put_object(
            #     Key=s3_key,
            #     Body=extracted_json,
            # )

            # print(s3_key)
        contents = s3_client.list_objects(Bucket=bucket)['Contents']
        list_of_files = [b["Key"] for b in contents]
        return list_of_files
    except ClientError as e:
        name = __name__
        log_message(name, "40", e.response["Error"]["Message"])
    finally:
        conn.close()
