import time
from .pipelines import (
    Pipeline, PipelineConfig
)
import pandas
import yaml
import hashlib
import os
from ftplib import FTP
import pysftp

from botocore.exceptions import ClientError
from datetime import datetime

class IntegrationPipeline(Pipeline):
    def __init__(self, config: PipelineConfig | str, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
        self.integration_config:dict = self.config.integration

    def _configure_pipeline(self, *args, **kwargs):
        pass

    def integrate(self,*args, **kwargs):
        """
        Integrate the data into the configured integration method.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.tracker.phase('Integration')
        if self.integration_config.get('mode','ftp').upper() == 'FTP':
            self._integrate_ftp()
        elif self.integration_config.get('mode','ftp').upper() == 'SFTP':
            self._integrate_sftp()
        elif self.integration_config.get('mode','ftp').upper() == 'API':
            self._integrate_api()
        else:
            self._get_last_from_path()
        self.tracker.finish()

    def _get_last_from_path(self):
        path = self.integration_config.get('path', '')
        extension = self.integration_config.get('extension', 'csv')
        is_s3 = str(path).startswith('s3://')
        last_created_file = None
        
        if is_s3:
            path = str(path).replace('s3://','')
            s3_client = self.env.storage
            bucket_name = path.split('/')[0]
            prefix = '/'.join(path.split('/')[1:]) + '/'
            try:
                self.logger.info(f'Listing files in {bucket_name}/{prefix}')
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            except ClientError as e:
                raise Exception(f"Could not list objects in S3 bucket {bucket_name}: {e}")
            
            if 'Contents' in response:
                last_created_file = max(response['Contents'], key=lambda x: x['LastModified'])['Key']
        else:
            if not os.path.exists(path):
                raise Exception(f"Local directory {path} does not exist")
            
            all_files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
            matching_files = [f for f in all_files if f.endswith(extension)]
            
            if matching_files:
                last_created_file = max(matching_files, key=lambda x: os.path.getctime(os.path.join(path, x)))
        
        if last_created_file is not None:
            if is_s3:
                s3_client = self.env.storage
                bucket_name = path.split('/')[0]
                filename = os.path.basename(last_created_file)
                target_file = os.path.join('/tmp', filename)
                try:
                    self.logger.info(f'Downloading {last_created_file} to {target_file}')
                    s3_client.download_file(bucket_name, last_created_file, target_file)
                except ClientError as e:
                    raise Exception(f"Could not download file {last_created_file} from S3 bucket {bucket_name}: {e}")
            else:
                source_file = os.path.join(path, last_created_file)
                try:
                    with open(source_file, 'rb') as f:
                        with open(target_file, 'wb') as g:
                            g.write(f.read())
                except OSError as e:
                    raise Exception(f"Could not download file {last_created_file} from local directory {path}: {e}")

            return target_file
    def _integrate_ftp(self):
        """
        Transfer files to an FTP server.
        """
        ftp_config = self.integration_config.get('credentials', {})
        ftp = FTP()
        ftp.connect(host=ftp_config.get('host', ''), port=ftp_config.get('port', 21))
        ftp.login(user=ftp_config.get('user', ''), passwd=ftp_config.get('password', ''))
        ftp.cwd(self.integration_config.get('path', ''))
        files = self._get_files_to_transfer()
        if self.integration_config.get('method') == 'put':
            for file_path in files:
                with open(file_path, 'rb') as file:
                    ftp.storbinary('STOR {}'.format(os.path.basename(file_path)), file)
                    self.logger.info('Uploaded {} to FTP server.'.format(file_path))
        else:
            raise ValueError('Invalid value for method in integration config.')

    def _integrate_sftp(self):
        """
        Transfer files to SFTP server.
        """
        self.logger.info('Integrating to SFTP')
        sftp_config = self.integration_config.get('credentials', {})
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(
                host=sftp_config.get('host', ''),
                port=sftp_config.get('port', 22),
                username=sftp_config.get('user', ''),
                password=sftp_config.get('password', ''),
                cnopts=cnopts) as sftp:
            target_path = self.integration_config.get('target_path', 'upload')
            self.logger.info(f'CWD to {target_path}')
            sftp.cwd(target_path)
            files = self._get_files_to_transfer()
            if self.integration_config.get('method') == 'put':
                for file_path in files:
                    self.logger.info(f'Uploading {file_path}')
                    sftp.put(file_path, os.path.basename(file_path))
                    self.logger.info('Uploaded {}'.format(file_path))
                    os.remove(file_path)
            else:
                raise ValueError('Invalid value for method in integration config.')

    def _get_files_to_transfer(self):
        discover_file = self.integration_config.get('discover_file', 'last_in_path')
        if discover_file == 'last_in_path':
            return [self._get_last_from_path()]
        else:
            raise ValueError('Invalid value for discover_file in integration config.')
    
    def _integrate_api(self):
        import requests
        import base64
        api_key = self.integration_config.get('api_key')
        auth_url = self.integration_config.get('auth_url')
        token_bearer = self.integration_config.get('token_bearer', 'Bearer')
        data = self._get_files_to_transfer()
        url = self.integration_config.get('url')
        method = self.integration_config.get('method', 'post')
        file_name_key = self.integration_config.get('file_name_key','file_name')
        file_data_key = self.integration_config.get('file_data_key','file_data')

        # Get token if auth_url is provided
        if auth_url:
            user = self.integration_config.get('credential','user')
            password = self.integration_config.get('password','password')
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            auth_response = requests.post(auth_url, data=api_key, headers=headers)
            if auth_response.status_code != 200:
                raise Exception(f"Authentication failed with status code {auth_response.status_code}: {auth_response.text}")
            access_token = auth_response.json().get('access_token')
            headers = {'Authorization': f'{token_bearer} {access_token}'}
        elif api_key:
            headers = {'Api-Key': f'{api_key}'}
        else:
            raise Exception(f"Either api-key or oauth2 method must be informed.")

        # Send request for each file in data
        for file_name, file_data in data.items():
            file_data_base64 = base64.b64encode(file_data).decode('utf-8')            
            headers.update({'Content-Type': 'application/json'})
            payload = {file_name_key: file_name, file_data_key: file_data_base64}
            response = requests.request(method=method, url=url, headers=headers, json=payload)
            if response.status_code not in [200, 201]:
                raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        