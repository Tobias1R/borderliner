import logging
import sys
import os

DEFAULT_FORMAT = '[%(asctime)s] %(levelname)s - %(message)s'


def get_logger(log_format=DEFAULT_FORMAT):
    # logging
    if os.getenv('pipeline_log_format',False):
        log_format = os.getenv('pipeline_log_format',DEFAULT_FORMAT)
    try:
        logging.basicConfig(
            stream=sys.stdout, 
            level=logging.INFO,
            format=log_format
            )
        return logging.getLogger()
    except Exception:
        logging.basicConfig(
            stream=sys.stdout, 
            level=logging.INFO,
            format=DEFAULT_FORMAT
            )
        return logging.getLogger()