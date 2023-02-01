
import sys
import os
import logging
import json
import yaml


# logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()

class CloudEnvironment:
    def __init__(self,source,*args,**kwargs) -> None:
        self.service = 'CLOUD'
        self.storage = {}
        self.connections = {}
        self.loaded_libs = {}
        self.show_info = True
        self.pipeline_db = None
        if isinstance(source,str):
            self._load_from_file(source)
        elif isinstance(source,dict):
            self._load_dict(source)
        else:
            raise ValueError("Impossible to configure cloud environment")
        self._show_info()
        
    def _load_dict(self,source):
        for key in source:
            self.__setattr__(key,source[key])

    def _show_info(self):
        logger.info(f'cloud enviroment {self.service} loaded.')
        if self.show_info:
            logger.info(json.dumps(self.connections,
                  sort_keys=True, indent=4))

    def _load_from_file(self,file):
        with open(file,'r') as f:
            data_loaded = yaml.safe_load(f)
            for key in data_loaded:
                self.__setattr__(key,data_loaded[key])

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