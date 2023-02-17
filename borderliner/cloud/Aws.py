from . import CloudEnvironment
import logging
import sys
import os
import boto3
from botocore.exceptions import ClientError
import io
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
        self.storage = boto3.client('s3')
        
    
    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        logger.info(f'Uploading file to bucket: s3://{bucket}/{file_name}')
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
    
    def download_flat_files(self,source_config):
        # Get the list of files in the directory
        response = self.storage.list_objects_v2(
            Bucket=source_config['storage']['directory'],
            Prefix=source_config['storage']['prefix'],
        )

        # Filter the list of files by extension
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(source_config['extension'])]

        # Sort the list of files by date (most recent first)
        files.sort(reverse=True)

        # Get the first file (most recent)
        
        if len(files) > 0:
            latest_file = files[1]
            print(latest_file)
            # Download the file
            if source_config.get('storage',{}).get('download_files',False):
                if not os.path.isdir('borderliner_downloads'):
                    os.mkdir('borderliner_downloads')

                self.storage.download_file(
                    Bucket=source_config['storage']['directory'],
                    Key=latest_file,
                    Filename=os.path.join('borderliner_downloads', os.path.basename(latest_file)),
                )
            else:
                # Download the file
                response = self.storage.get_object(
                    Bucket=source_config['storage']['directory'],
                    Key=latest_file
                )

                # Read the contents of the file into a buffer
                data_buffer = io.BytesIO(response['Body'].read())
                self.data_buffers.append(data_buffer)

            # Move the file to the archive
            if source_config['move_files_to_archive']:
                archive_prefix = source_config['archive']['prefix'].rstrip('/')
                archive_key = f"{archive_prefix}/{os.path.basename(latest_file)}"

                self.storage.copy_object(
                    Bucket=source_config['archive']['directory'],
                    CopySource=f"{source_config['storage']['directory']}/{latest_file}",
                    Key=archive_key,
                )

            # Delete the file if configured to do so
            if source_config['remove_files']:
                self.storage.delete_object(
                    Bucket=source_config['storage']['directory'],
                    Key=latest_file,
                )
    
