import pytest
from src.extract import extract_from_db
from moto import mock_aws
from unittest import mock
import boto3
from src.extract import extract_from_db
from utilities.utils import create_s3_bucket, upload_to_s3, \
    view_bucket_contents, credentials_storer, secret_retriever

@pytest.fixture()
def s3_resource():
    with mock_aws():
        yield boto3.resource('s3')


@pytest.fixture()
def secretsmanager_client():
    with mock_aws():
        yield boto3.client('secretsmanager')


@pytest.mark.skip()
def test_get_bucket_contents(s3_resource):
    bucket = create_s3_bucket('test-bucket', s3_resource=s3_resource)
    upload_to_s3('test.txt', 'test-bucket')
    upload_to_s3('test2.txt', 'test-bucket')
    assert view_bucket_contents(bucket) == ['test.txt', 'test2.txt']


def test_extract(s3_resource):   
    bucket = create_s3_bucket('test-bucket', s3_resource=s3_resource)
    print(bucket)
    assert extract_from_db('test-bucket', s3_resource=s3_resource) == view_bucket_contents(bucket)

@pytest.mark.skip()
def test_get_secret(secretsmanager_client):
    credentials_storer('secret', 'bob', 'marley', secretsmanager_client)
    assert secret_retriever('secret') == '''{
    "username":"userId",
    "password":"password"
}'''