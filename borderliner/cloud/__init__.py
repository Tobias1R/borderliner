
import sys
import os
import logging
import json
from typing import List
import yaml
import paramiko
from ftplib import FTP

def list_ftp_directory(ftp: FTP, path: str = "") -> List[str]:
    """
    List contents of a directory in FTP.

    Args:
        ftp: An active FTP connection.
        path: The path of the directory to list.

    Returns:
        A list of strings representing the contents of the directory.
    """
    if path:
        ftp.cwd(path)
    return ftp.nlst()
    
def list_files_sftp(host, port, username, password, remote_dir):
    """
    Lists the files in the specified remote SFTP directory.
    """
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    files = sftp.listdir(remote_dir)
    sftp.close()
    transport.close()
    return files

# logging
from borderliner.core.logs import get_logger
logger = get_logger()

class CloudEnvironment:
    def __init__(self,source,*args,**kwargs) -> None:
        self.service = 'CLOUD'
        self.storage = {}
        self.storage_paths = {}
        self.connections = {}
        self.loaded_libs = {}
        self.show_info = True
        self.pipeline_db = None
        self.data_buffers = []
        self.build_info_file = None
        self.build_version = '0.0.0'
        self.manager_type = 'AIRFLOW'
        self.manager = None
        self.task_manager_api =''
        self.task_manager_api_user=''
        self.task_manager_api_password=''
        self.default_bucket = ''
        if isinstance(source,str):
            self._load_from_file(source)
        elif isinstance(source,dict):
            self._load_dict(source)
        else:
            raise ValueError("Impossible to configure cloud environment")
        self._load_manager(kwargs.get('manager',None))
        self._show_info()

    
    def print_environment(self):
        # print os environment
        logger.info('OS ENVIRONMENT:')
        for key in os.environ:
            logger.info(f'{key}={os.environ[key]}')

        
    
    def _load_manager(self,manager=None):
        if manager:
            self.manager = manager
            return 
        
        if str(self.manager_type).upper() == 'AIRFLOW':
            from borderliner.cloud.flow_gateways.airflow_api import AirflowAPI
            self.manager = AirflowAPI(
                    base_url=self.task_manager_api,
                    username=self.task_manager_api_user,
                    password=self.task_manager_api_password
                )
            return
        
        logger.warning(f'No flow manager loaded.')
    
        
    def _load_dict(self,source):
        for key in source:
            self.__setattr__(key,source[key])
        data_loaded = source
        for key in data_loaded:
            # search for $env vars
            if isinstance(data_loaded[key],dict):
                for k in data_loaded[key]:                    
                    if str(data_loaded[key][k]).startswith('$ENV_'):
                        env_key = str(data_loaded[key][k]).replace('$ENV_','')#str(key) + '_' + str(k)
                        data_loaded[key][k] = os.getenv(
                                env_key,
                                data_loaded[key][k]
                            )   
                    elif str(data_loaded[key][k]).startswith('$airflow'):
                        env_key = 'AIRFLOW_VAR_'+str(key).upper() + '_' + str(k).upper()
                        data_loaded[key][k] = os.getenv(
                                env_key,
                                data_loaded[key][k]
                            ) 
                        print('LOADED ',env_key,data_loaded[key][k]) 
            elif isinstance(data_loaded[key],str):     
                if str(data_loaded[key]).startswith('$ENV_'):
                    env_key = str(data_loaded[key]).replace('$ENV_','')
                    data_loaded[key] = os.getenv(
                            env_key,
                            data_loaded[key]
                        )
            self.__setattr__(key,data_loaded[key])

    def _show_info(self):
        # get version info from file
        if self.build_info_file:
            with open(self.build_info_file,'r') as f:
                # read 1 line
                build_info = f.read()
                #log build info
                logger.info(f'GIT Commit Version: {build_info}')
                self.build_version = build_info
                
        logger.info(f'cloud enviroment {self.service} loaded.')
        if self.show_info:
            logger.info(json.dumps(self.connections,
                  sort_keys=True, indent=4))
        self.print_environment()

    # def _load_from_file(self,file):
    #     with open(file,'r') as f:
    #         data_loaded = yaml.safe_load(f)
    #         for key in data_loaded:
    #             self.__setattr__(key,data_loaded[key])
    
    def _load_from_file(self,file):
        data_loaded = yaml.safe_load(file)
        return self._load_dict(data_loaded)

    def _load_connection_interfaces(self):
        sys.path.insert(1, os.getcwd())
        
        for key in self.connections:
            conn = self.connections[key]
            if 'lib' in conn:
                libfile = conn['lib']
                try:
                    c:conn_abstract.PipelineConnection = import_module(
                        libfile,
                        package='craftable').Connection
                    logger.info(f'interface module {c} loaded.')
                    self.loaded_libs[key] = c
                except Exception as e:
                    logger.warning(f'failed to load module {libfile}')
                    #print(e)

    def _connect_pipeline_db(self):
        self.database = ''
    
    def save_to_database(self,*args,**kwargs):
        pass

    def save_dataframe_to_csv_storage(self,datasource,file,*args,**kwargs):
        logger.info(f'dumping query results to csv {file}')

    def copy_csv_storage_to_database(self,file,table,conn,*args,**kwargs):
        logger.info(f'[{conn}] copying {file} to table {table}')
    
    def get_connection(self,connection_name):
        if connection_name in self.connections:
            return self.connections[connection_name]
        raise ConnectionNotFoundException()
    
    def get_connection_driver(self,driver_name):
        if driver_name in self.loaded_libs:
            return self.loaded_libs[driver_name]
        raise ConnectionNotFoundException()
    
    def show_connections(self):
        print(self.connections)
    
    def upload_file_to_storage(self,file_name, storage_root, object_name,*args, **kwargs):
        pass
    
    def list_directory(self, directory_path,**kwargs):
        """
        Returns a list of objects in the specified directory.
        """
        if self.service == 'S3':
            bucket = self.storage.get('bucket')
            objects = self.connections['s3'].list_objects_v2(Bucket=bucket, Prefix=directory_path,**kwargs)['Contents']
            return [obj['Key'] for obj in objects]
        elif self.service == 'GCP':
            bucket = self.storage.get('bucket')
            blobs = self.connections['client'].list_blobs(bucket_or_name=bucket, prefix=directory_path,**kwargs)
            return [blob.name for blob in blobs]
        elif self.service == 'AZURE':
            container_client = self.connections.get('container_client')
            blob_list = container_client.list_blobs(name_starts_with=directory_path,**kwargs)
            return [blob.name for blob in blob_list]
        elif self.service == 'CLOUD':
            # implementation for simple storage here
            ftp = FTP(
                self.connections['host'],
                #self.connections['port'],
                self.connections['user'],
                self.connections['password']
            )
            return list_ftp_directory(ftp,directory_path)
        elif self.service == 'CLOUDSFTP':
            return list_files_sftp(
                self.connections['host'],
                self.connections['port'],
                self.connections['user'],
                self.connections['password'],
                directory_path
            )
        else:
            raise ValueError("Invalid cloud service specified.")
    
    def download_flat_files(self,source_config):
        pass

    def move_file(self,source,destination):
        pass

    