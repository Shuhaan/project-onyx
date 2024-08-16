from pg8000 import native
from pg8000.exceptions import DatabaseError
from extract_lambda.utils import get_secret, log_message
import boto3


def connect_to_db():
    try:
        credentials = get_secret()
        log_message(__name__, 20, "Retrieved secrets from Secret Manager for DB access")
        return native.Connection(
            user=credentials["USERNAME"],
            password=credentials["PASSWORD"],
            database=credentials["DBNAME"],
            host=credentials["HOST"],
            port=int(credentials["PORT"]),
        )

    except DatabaseError as e:
        log_message(__name__, 40, e.response["Error"]["Message"])
        raise e
