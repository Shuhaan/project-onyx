from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from load_utils import get_secret, log_message


def connect_to_warehouse() -> Connection:
    try:
        credentials = get_secret()
        log_message(
            __name__, 20, "Retrieved secrets from Secrets Manager for DB access"
        )
        return Connection(
            user=credentials["USERNAME"],
            password=credentials["PASSWORD"],
            database=credentials["DBNAME"],
            host=credentials["HOST"],
            port=int(credentials["PORT"]),
        )

    except DatabaseError as e:
        log_message(__name__, 40, f"DatabaseError: {e}")
        raise e