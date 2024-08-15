import pytest
import json
from moto import mock_aws
import boto3
import os
from dotenv import load_dotenv
from src.extract import extract_from_db_write_to_s3
from pprint import pprint