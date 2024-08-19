import pytest
from unittest.mock import patch


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
def mock_db_connection():
    return MockedConnection()


@pytest.fixture()
def patch_db_connection(mock_db_connection):
    with patch("extract_lambda.extract.connect_to_db", return_value=MockedConnection()):
        yield mock_db_connection
