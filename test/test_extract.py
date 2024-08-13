import pytest
from src.extract import extract_from_db
from moto import mock_aws
from unittest import mock
import boto3