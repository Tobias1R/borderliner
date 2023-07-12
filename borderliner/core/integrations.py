import json
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
        kwargs['no_source'] = True
        kwargs['no_target'] = True
        super().__init__(config, *args, **kwargs)
        self.integration_config:dict = self._replace_env_keys(self.config.integration)
        self.raise_on_error = self.integration_config.get('raise_on_error', True)

    
    def _replace_env_keys(self, config: dict) -> dict:
        for key in config:
            
            if isinstance(config[key], dict):
                
                config[key] = self._replace_env_keys(config[key])
            elif isinstance(config[key], str):
                
                if str(config[key]).startswith('$ENV_'):
                    env_key = str(config[key]).replace('$ENV_','')
                    config[key] = os.getenv(
                            env_key,
                            config[key]
                        )
        return config

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
        ftp_config = self._replace_env_keys(self.integration_config.get('credentials', {}))
        
        ftp = FTP()
        ftp.connect(host=ftp_config.get('host'), port=int(ftp_config.get('port',21)))
        self.logger.info('Connected to FTP server {}.'.format(ftp_config.get('host')))
        ftp.login(user=ftp_config.get('user'), passwd=ftp_config.get('password'))
        self.logger.info('Logged in to FTP server {}.'.format(ftp_config.get('host')))
        remote_path = ftp_config.get('remote_path', False)
        if remote_path:
            ftp.cwd(remote_path)
        files = self._get_files_to_transfer()
        self.logger.info(f'Files to transfer: {files}')
        if self.integration_config.get('method') == 'put':
            if isinstance(files, str):
                files = [files]
            for file_path in files:
                with open(file_path, 'rb') as file:
                    self.logger.info('Uploading {} to FTP server.'.format(file_path))
                    ftp.storbinary('STOR {}'.format(os.path.basename(file_path)), file)
                    self.logger.info('Uploaded {} to FTP server.'.format(file_path))
        else:
            raise ValueError('Invalid value for method in integration config.')
        ftp.quit()

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
    
    def _get_path_from_xcom(self):
        xcom_variable = self.integration_config.get('xcom_variable')
        if xcom_variable is None:
            raise ValueError('xcom_variable must be specified in integration config.')
        if str(xcom_variable).strip() == '':
            return self._get_last_from_path()
        return [os.getenv(xcom_variable)]

    def _get_files_to_transfer(self):
        discover_file = self.integration_config.get('discover_file', 'last_in_path')
        if discover_file == 'last_in_path':
            return [self._get_last_from_path()]
        elif discover_file == 'path_from_xcom':
            return self._get_path_from_xcom()
        else:
            raise ValueError('Invalid value for discover_file in integration config.')
    
    def parse_api_response(self, response,filename=None):
        # check if is json
        try:
            return response.json()
        except:
            pass
        return None

    def _integrate_api(self):
        import requests
        import base64
        auth_section = self.integration_config.get('auth', {})
        auth_type = auth_section.get('type', 'basic')
        auth_response = None
        token = ''
        if auth_type == 'basic':
            user = auth_section.get('username')
            password = auth_section.get('password')
            grant_type = auth_section.get('grant_type', 'password')
            content_type = auth_section.get('content_type', 'application/x-www-form-urlencoded')
            auth_header = auth_section.get('auth_header', None)
            auth_method = auth_section.get('auth_method', 'post')

            if grant_type == 'password':
                auth_payload = f'grant_type={grant_type}&username={user}&password={password}'
            elif grant_type == 'client_credentials':
                auth_payload = f'grant_type={grant_type}'
            else:
                raise ValueError('Invalid value for grant_type in integration config.')
            
            if auth_header:
                headers = auth_header
            else:
                headers = {'Content-Type': content_type}
            self.logger.info(f'Auth payload: {auth_payload}')
            self.logger.info(f'Auth headers: {headers}')
            auth_response = requests.request(
                method=auth_method,
                url=auth_section.get('auth_url'),
                data=auth_payload,
                headers=headers
            )
            print(auth_response)
            response_json = auth_response.json()
            token = response_json.get(auth_section.get('token_key', 'access_token'), '')
            self.logger.info(f'Auth response: {auth_response}')
            if auth_response.status_code != 200:
                raise ValueError('Invalid response from auth server.')
        elif auth_type == 'bearer':
            auth_response = auth_section.get('token')
        else:
            raise ValueError('Invalid value for auth_type in integration config.')
        
        data = self._get_files_to_transfer()
        url = self.integration_config.get('url')
        method = self.integration_config.get('method', 'post')
        payload_section = self.integration_config.get('payload', {})
        if isinstance(data, str):
            data = [data]
        
        for filename in data:
            if os.path.isfile(filename):
                with open(filename, 'rb') as file:
                    file_data = file.read()
                    if payload_section.get('base64', False):
                        file_data = base64.b64encode(file_data).decode('UTF-8')
                    payload = payload_section.get('payload', {})
                    payload.update({
                        payload_section.get('file_name_key', 'name'): os.path.basename(filename),
                        payload_section.get('file_data_key', 'file'): file_data
                    })
                    payload = json.dumps(payload)
                    headers = payload_section.get('headers', {})
                    if auth_type == 'bearer':
                        headers.update({'Authorization': f'Bearer {response_json.get("access_token", "")}'})
                    elif auth_type == 'basic':
                        headers.update({'Authorization': f'{response_json.get("token_type", "Bearer")} {token}'})
                    self.logger.info(f'Payload: {payload}')
                    self.logger.info(f'Headers: {headers}')
                    response = requests.request(
                        method=method,
                        url=url,
                        data=payload,
                        headers=headers
                    )
                    self.response_json = self.parse_api_response(response,filename)
                    self.logger.info(f'Response: {response.status_code}')
                    if self.raise_on_error and response.status_code not in [200,201]:
                        self.logger.error(f'Error: {response.text}')
                        raise ValueError('Invalid response from server.')
                    



    
        