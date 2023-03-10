from . import CloudEnvironment
import logging
import sys
import os
import io
from minio import Minio
from minio.error import ResponseError

class DatacenterMinioStorageEnvironment(CloudEnvironment):
    def __init__(self, source, *args, **kwargs) -> None:
        super().__init__(source, *args, **kwargs)
        self.storage = Minio(
            endpoint=source['endpoint'],
            access_key=source['access_key'],
            secret_key=source['secret_key'],
            secure=source.get('secure', True),
            region=source.get('region', None)
        )

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
            self.storage.fput_object(bucket, object_name, file_name)
        except ResponseError as e:
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

