import pytest
import os
import boto3
from moto import mock_aws
from src.utils import get_secret


@pytest.fixture(scope='class')
def aws_creds():
    os.environ ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ ['AWS_DEFAULT_REGION'] = 'eu-west-2'
    
@pytest.fixture()
def secretsmanager_client():
    with mock_aws():
        yield boto3.client('secretsmanager')


def test_get_secret(secretsmanager_client):
    secretsmanager_client.create_secret(
        Name="aSecret", SecretString=str(
            '''{
                    "username":"userId",
                    "password":"password"
                }'''
        )
    )
    response = get_secret(secret_name="aSecret",region_name="eu-west-2")
    assert response == {
                        "username":"userId",
                        "password":"password"
                        }