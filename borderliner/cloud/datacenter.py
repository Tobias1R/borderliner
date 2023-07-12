import shutil
from . import CloudEnvironment
import logging
import sys
import os
import io
from minio import Minio
from minio.error import InvalidResponseError

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DatacenterMinioStorageEnvironment(CloudEnvironment):
    def __init__(self, source, *args, **kwargs) -> None:
        super().__init__(source, *args, **kwargs)
        self.storage = None

    def upload_file(self, file_name, bucket, object_name=None):
        """Upload a file to a Minio bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: Minio object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        self.logger.info(f'Uploading file to bucket: s3://{bucket}/{file_name}')
        # If Minio object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            self.storage.put_object(bucket, object_name, file_name)
        except InvalidResponseError as e:
            logging.error(e)
            return False
        return True

    def upload_file_to_storage(self, file_name, storage_root, object_name, *args, **kwargs):
        self.upload_file(
            file_name=file_name,
            bucket=storage_root,
            object_name=object_name)

    def copy_csv_storage_to_database(self, file, table, conn, *args, **kwargs):
        return super().copy_csv_storage_to_database(file, table, conn, *args, **kwargs)
    
    def download_flat_files(self, source_config):
        # Get the list of files in the directory
        objects = self.storage.list_objects_v2(
            source_config['storage']['directory'],
            prefix=source_config['storage']['prefix'],
        )

        # Filter the list of files by extension
        files = [obj.object_name for obj in objects if obj.object_name.endswith(source_config['extension'])]

        # Sort the list of files by date (most recent first)
        files.sort(reverse=True)

        # Get the first file (most recent)
        if len(files) > 0:
            latest_file = files[1]
            print(latest_file)

            # Download the file
            if source_config.get('storage', {}).get('download_files', False):
                if not os.path.isdir('borderliner_downloads'):
                    os.mkdir('borderliner_downloads')

                self.storage.fget_object(
                    source_config['storage']['directory'],
                    latest_file,
                    os.path.join('borderliner_downloads', os.path.basename(latest_file)),
                )
            else:
                # Download the file
                data = self.storage.get_object(
                    source_config['storage']['directory'],
                    latest_file,
                ).read()

                # Read the contents of the file into a buffer
                data_buffer = io.BytesIO(data)
                self.data_buffers.append(data_buffer)

            # Move the file to the archive
            if source_config['move_files_to_archive']:
                archive_prefix = source_config['archive']['prefix'].rstrip('/')
                archive_key = f"{archive_prefix}/{os.path.basename(latest_file)}"

                self.storage.copy_object(
                    source_config['archive']['directory'],
                    archive_key,
                    f"{source_config['storage']['directory']}/{latest_file}",
                )

            # Delete the file if configured to do so
            if source_config['remove_files']:
                self.storage.remove_object(
                    source_config['storage']['directory'],
                    latest_file,
                )


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
class DataCenterS3Environment(CloudEnvironment):
    def __init__(self, source, *args, **kwargs) -> None:
        super().__init__(source, *args, **kwargs)
        self.service = 'Datacenter with S3 storage'
        try:
            self.storage = boto3.client('s3',
                        endpoint_url=os.getenv('storage_host'),
                        aws_access_key_id=os.getenv('storage_login'),
                        aws_secret_access_key=os.getenv('storage_password'),
                        verify=False)
        except Exception as e:
            self.storage = None
        
    
    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        logger.info(f'Uploading file to bucket: s3://{bucket}/{object_name}')
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = self.storage
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

        if not os.path.isdir('borderliner_downloads'):
            os.mkdir('borderliner_downloads')

        # check if response is empty
        if not response.get('Contents'):
            logger.info(f'No files found in bucket: s3://{source_config["storage"]["directory"]}/{source_config["storage"]["prefix"]}')
            return False

        # Filter the list of files by extension
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(source_config['extension'])]

        # Sort the list of files by date (most recent first)
        files.sort(reverse=True)

        # Get the first file (most recent)

        if source_config.get('file_pattern',None) == '*':
            # download all files
            for file in files:
                logger.info(f'Downloading file: s3://{source_config["storage"]["directory"]}/{file}')
                self.storage.download_file(
                    Bucket=source_config['storage']['directory'],
                    Key=file,
                    Filename=os.path.join('borderliner_downloads', os.path.basename(file)),
                )
        else:
        
            if len(files) > 0:
                latest_file = files[1]
                print(latest_file)
                # Download the file
                if source_config.get('storage',{}).get('download_files',False):
                    

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
                if source_config.get('move_files_to_archive', False):
                    archive_prefix = source_config['archive']['prefix'].rstrip('/')
                    archive_key = f"{archive_prefix}/{os.path.basename(latest_file)}"

                    self.storage.copy_object(
                        Bucket=source_config['archive']['directory'],
                        CopySource=f"{source_config['storage']['directory']}/{latest_file}",
                        Key=archive_key,
                    )

                # Delete the file if configured to do so
                if source_config.get('remove_files', False):
                    self.storage.delete_object(
                        Bucket=source_config['storage']['directory'],
                        Key=latest_file,
                    )
    
    def save_file(self, file_name, bucket=None, object_name=None):
        if bucket is None:
            bucket = self.default_bucket
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            # remove first /
            if object_name.startswith('/'):
                object_name = object_name[1:]
            self.upload_file(file_name, bucket, object_name)
        except Exception as e:
            logger.warning(f'Failed to save file {file_name} to bucket {bucket}/{object_name}. Error: {e}')   
            # save file on /files using object_name as path            
            if object_name:
                if str(object_name).startswith('/files'):
                    pass
                else:
                    object_name = object_name.replace(' ','_')
                    # copy file to /files
                    shutil.copyfile(file_name, os.path.join('/files',object_name))
            else:
                shutil.copyfile(file_name, os.path.join('/files',os.path.basename(file_name)))
            
    def move_file(self,source,destination):
        try:
            self.storage.copy_object(
                Bucket=self.default_bucket,
                CopySource=f"{self.default_bucket}/{source}",
                Key=destination,
            )
            self.storage.delete_object(
                Bucket=self.default_bucket,
                Key=source,
            )
        except Exception as e:
            logger.warning(f'Failed to move file {source} to {destination}. Error: {e}')
    
    def delete_file_from_storage(self, bucket, object_name):
        try:
            self.storage.delete_object(
                Bucket=bucket,
                Key=object_name,
            )
        except Exception as e:
            logger.warning(f'Failed to delete file {object_name} from bucket {bucket}. Error: {e}')