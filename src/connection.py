from pg8000 import native
from pg8000.exceptions import DatabaseError
from utils import get_secret
import boto3


def connect_to_db():
    try:
        credentials = get_secret()
        return native.Connection(
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
