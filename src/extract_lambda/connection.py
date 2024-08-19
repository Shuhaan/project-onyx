from pg8000 import native
from pg8000.exceptions import DatabaseError
from extract_lambda.utils import get_secret, log_message


def connect_to_db() -> native.Connection:
    """
    Establishes a connection to the PostgreSQL database using credentials
    retrieved from AWS Secrets Manager.

    This function fetches database connection details from AWS Secrets Manager,
    and uses these details to create and return a connection object to the PostgreSQL
    database. If an error occurs during connection, it logs the error and raises an exception.

    :return: A pg8000 native.Connection object.
    :raises DatabaseError: If there is an issue with the database connection.
    """
    try:
        credentials = get_secret()
        log_message(
            __name__, 20, "Retrieved secrets from Secrets Manager for DB access"
        )
        return native.Connection(
            user=credentials["USERNAME"],
            password=credentials["PASSWORD"],
            database=credentials["DBNAME"],
            host=credentials["HOST"],
            port=int(credentials["PORT"]),
        )

    except DatabaseError as e:
        log_message(__name__, 40, f"DatabaseError: {e}")
        raise e
