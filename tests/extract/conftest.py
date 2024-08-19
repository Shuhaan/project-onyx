import pytest, boto3, json
from moto import mock_aws
from unittest.mock import patch


@pytest.fixture()
def secretsmanager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture()
def create_secrets(secretsmanager_client):
    # load_dotenv()
    secret_string = {
        "USERNAME": "user",  # os.getenv("USERNAME")
        "PASSWORD": "pass",  # os.getenv("PASSWORD")
        "HOST": "host",  # os.getenv("HOST")
        "PORT": 5432,  # os.getenv("PORT")
        "DBNAME": "db",  # os.getenv("DATABASE")
    }
    secret = json.dumps(secret_string)
    secretsmanager_client.create_secret(
        Name="project-onyx/totesys-db-login", SecretString=secret
    )
    return secretsmanager_client


class MockedConnection:
    def __init__(
        self, user="user", password="pass", database="db", host="host", port=5432
    ):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.columns = [
            {"name": "data_id"},
            {"name": "meaningful_data"},
            {"name": "last_updated"},
        ]
        self.rows_data1 = [
            ["1", "old_data1", "1970-01-01 20:00:00"],
            ["2", "old_data2", "1970-01-01 20:00:00"],
        ]

        self.rows_data2 = [
            ["1", "new_data1", "1970-01-01 20:00:05"],
            ["2", "new_data2", "1970-01-01 20:00:05"],
        ]

    def run(self, query):
        if "WHERE" in query:
            return self.rows_data2
        return self.rows_data1

    def close(self):
        pass


@pytest.fixture()
def patch_db_connection():
    with patch("extract_lambda.extract.connect_to_db", return_value=MockedConnection()):
        yield MockedConnection()
