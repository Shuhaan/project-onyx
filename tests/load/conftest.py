import pytest, boto3, json
from pg8000.exceptions import DatabaseError
from moto import mock_aws
from unittest.mock import patch
from load_utils import log_message


@pytest.fixture()
def db_credentials_fail(
    user="loser", password="TEST", database="dbz", host="club", port=1234
):
    return (user, password, database, host, port)

