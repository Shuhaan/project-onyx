from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from src.utils import get_secret
import boto3


def connect_to_db():
    try:
        credentials = get_secret()
        return Connection(
            user=credentials["username"],
            password=credentials["password"],
            database=credentials["dbname"],
            host=credentials["host"],
            port=int(credentials["port"]),
        )

    except DatabaseError as e:
        # need to call logging function
        print(e)
        raise e
