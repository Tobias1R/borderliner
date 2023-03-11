from . import CloudEnvironment
import logging
import sys
import os
import boto3
from botocore.exceptions import ClientError
import io
DEFAULT_FORMAT = '[%(asctime)s] %(levelname)s - %(message)s'


def get_logger():
    # logging
    if os.getenv('pipeline_log_format',False):
        log_format = os.getenv('pipeline_log_format',DEFAULT_FORMAT)
    logging.basicConfig(
        stream=sys.stdout, 
        level=logging.INFO,
        format=log_format
        )
    return logging.getLogger()