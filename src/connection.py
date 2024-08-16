from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from utils import get_secret, log_message
import boto3


def connect_to_db():
    try:
        credentials = get_secret()
        log_message(__name__, 20, "Retrieved secrets from Secret Manager for DB access")
        return Connection(
            user=credentials["username"],
            password=credentials["password"],
            database=credentials["dbname"],
            host=credentials["host"],
            port=int(credentials["port"]),
        )

    except DatabaseError as e:
        log_message(__name__, 40, e.response["Error"]["Message"])
        raise e
