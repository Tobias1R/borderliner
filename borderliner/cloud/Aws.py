from . import CloudEnvironment
import logging
import sys
import os
import boto3
from botocore.exceptions import ClientError

# logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()
class AwsEnvironment(CloudEnvironment):
    def __init__(self, source, *args, **kwargs) -> None:
        super().__init__(source, *args, **kwargs)
    
    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        logger.info('Uploading file to bucket.')
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def upload_file_to_storage(self,file_name, storage_root, object_name,*args, **kwargs):
        self.upload_file(
            file_name=file_name, 
            bucket=storage_root, 
            object_name=object_name)

    def copy_csv_storage_to_database(self, file, table, conn, *args, **kwargs):
        return super().copy_csv_storage_to_database(file, table, conn, *args, **kwargs)
    
