import io
import os
import pandas
import logging
import sys

from borderliner.db.conn_abstract import DatabaseBackend
from borderliner.cloud import CloudEnvironment

# logging
from borderliner.core.logs import get_logger
logger = get_logger()

class PipelineSource:
    def __init__(self,config:dict,*args,**kwargs) -> None:
        self.kwargs = kwargs
        self.pipeline_pid = self.kwargs.get('pipeline_pid',0)
        self.csv_chunks_files = []
        self.logger = logger
        self.config = config
        self._data:pandas.DataFrame|list = None
        self.chunk_size = config.get('chunk_size',-1)
        self.metrics:dict = {
            'total_rows':0
        }
        self.pipeline_config = self.kwargs.get('pipeline_config',None)
        self.pipeline_name:str = self.kwargs.get('pipeline_name',False)
        if not self.pipeline_name:
            self.pipeline_name = str(self.pipeline_pid)
            self.logger.warning('Pipeline source did not received the pipeline_name, using pid.')
        self.pipeline_name = self.pipeline_name.replace(' ','_')
        self.control_columns_function = kwargs.get('control_columns_function',None)
    
    def extract(self):
        extracted_rows = self.metrics['total_rows']
        self.logger.info(f'Extracted {extracted_rows} rows')

    def __str__(self) -> str:
        return str(self.config['type']).upper()

    @property
    def data(self):
        source_empty = True
        
        if len(self.csv_chunks_files) > 0:
            return self.csv_chunks_files

        if isinstance(self._data, type(iter([]))):
            source_empty = False
        elif isinstance(self._data,list):
            if len(self._data) > 0:
                source_empty = False
        elif isinstance(self._data,pandas.DataFrame):
            source_empty = self._data.empty
        elif self._data == None:
            source_empty = True
        else:
            raise ValueError('Invalid data in SOURCE')
        if not source_empty:
            return self._data
            
        self.extract()
        
        return self._data
    
    def inspect_source(self):
        self.logger.info("Inspecting data source")
    
    

class PipelineSourceDatabase(PipelineSource):
    def __init__(self, config: dict,*args,**kwargs) -> None:
        super().__init__(config,*args,**kwargs)
        self.database_module = 'psycopg2'
        self.alchemy_engine_flag = 'psycopg2'
        self.driver_signature = ''
        self.backend:DatabaseBackend = None
        self.user:str = ''
        self.database:str = ''
        self.password:str = ''
        self.host:str = ''
        self.port:str = None
        self.queries = {}
        self.table = config.get('table','')
        self.count = 0
        self.total_time = 0.0
        self.schema = config.get('schema','')
        self.engine = None
        self.connection = None

        self.kwargs = kwargs

        
        self.iteration_list = []
        self.deltas = {}
        self.primary_key = ()

        self.configure()
    
    def inspect_source(self):
        super().inspect_source()
        table_name = f'{self.table}'
        return self.backend.inspect_table(self.schema,table_name)
    
    def configure(self):
        self.user = self.config['username']
        self.password = self.config['password']
        self.host = self.config['host']
        self.port = self.config['port']
        match str(self.config['type']).upper():
            # TODO: dynamically import
            # i'll leave these dbs as native support.
            case 'POSTGRES':
                from borderliner.db.postgres_lib import PostgresBackend
                self.backend = PostgresBackend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
            case 'REDSHIFT':
                from borderliner.db.redshift_lib import RedshiftBackend
                self.backend = RedshiftBackend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
            case 'IBMDB2':
                from borderliner.db.ibm_db2_lib import IbmDB2Backend
                self.backend = IbmDB2Backend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
        self.queries = self.config['queries']
        self.engine = self.backend.get_engine()
        self.connection = self.backend.get_connection()
    
    def populate_deltas(self):
        pass

    def populate_iteration_list(self):
        df = pandas.read_sql_query(
            self.queries['iterate'],
            self.engine
        )

        
        total_cols = int(df.shape[1])
        self._data = []
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        df = df.to_dict(orient='records')
        slice_index = 1
        for item in df:
            self.logger.info(f'Extract by iteration: {item}')
            query = self.queries['extract'].format(
                **item
            )
            data = pandas.read_sql_query(query,self.engine)
            self.metrics['total_rows'] += len(data)
            if self.kwargs.get('dump_data_csv',False):
                filename_csv = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.csv' 
                filename_parquet = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                if self.control_columns_function:
                    data = self.control_columns_function(data)     
                #if self.kwargs.get('dump_data_csv',False)  == 'CSV':  
                filename = filename_parquet      
                data.to_parquet(
                    filename_parquet,
                    index=False
                )
                self.csv_chunks_files.append(filename)
                #self._data.append(data)
            else:
                self._data.append(data)
            slice_index += 1
        

    def extract_by_iteration(self):
        self.populate_iteration_list()

    def extract(self):
        if 'iterate' in self.queries:
            self.extract_by_iteration()
            super().extract()
            return
        
        if self.chunk_size > 0: 
            data = pandas.read_sql_query(
                self.get_query('extract'),
                self.engine,
                chunksize=self.chunk_size)
            
            slice_index = 1
            if self.kwargs.get('dump_data_csv',False):
                for df in data:
                    
                    filename = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                    if self.control_columns_function:
                        df = self.control_columns_function(df)                 
                    df.to_parquet(
                        filename,
                        index=False
                    )
                    self.csv_chunks_files.append(filename)
                    slice_index += 1
                    self.metrics['total_rows'] += len(df)
            else:
                self._data = data
        else:
            data = pandas.read_sql_query(
                self.get_query('extract'),
                self.engine)
            self.metrics['total_rows'] += len(data)
            if self.kwargs.get('dump_data_csv',False):
                filename = f'{self.pipeline_name}_slice_FULL.parquet'
                if self.control_columns_function:
                    
                    data = self.control_columns_function(data)                 
                data.to_parquet(
                    filename,
                    index=False
                )
                self.csv_chunks_files.append(filename)
            else:    
                self._data = data
        
        return super().extract()    

    def get_query(self,query:str='extract'):
        #print(self.queries)
        if query in self.queries:
            key_params = str(query)+'_params'
            if key_params in self.queries:
                return self.queries[query].format(
                    **self.queries[key_params]
                )
            else:
                return self.queries[query]
        
        
        raise Exception('Query not found.')




import requests
class PipelineSourceApi(PipelineSource):
    def __init__(self, config: dict, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
        # TODO: Make sure they exists 
        self.auth_type = config['api'].get('auth', {}).get('type', None)
        self.client_id = config['api'].get('auth', {}).get('client_id', None)
        self.client_secret = config['api'].get('auth', {}).get('client_secret', None)
        self.access_token_url = config['api'].get('auth', {}).get('access_token_url', None)
        self.bearer = config['api'].get('auth', {}).get('bearer', None)
        self.auth_headers_extra = config['api'].get('auth', {}).get('auth_headers_extra', None)

        # request
        self.request_headers = config.get('api', {}).get('request', {}).get('headers')
        self.request_method = config.get('api', {}).get('request', {}).get('method')
        self.request_url = config.get('api', {}).get('request', {}).get('url')
        self.request_data = config.get('api', {}).get('request', {}).get('data')
        self.request_read_json_params = config.get('api', {}).get('request', {}).get('read_json_params')

    def get_access_token(self):
        if not self.access_token:
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            for key,value in self.auth_headers_extra:
                data[key] = value
            response = requests.post(self.access_token_url, data=data)
            response.raise_for_status()
            self.access_token = response.json()['access_token']
        return self.access_token

    def make_request_oauth2(self, url, method='GET', data=None, headers=None):
        headers = headers or {}        
        for key,value in self.request_headers.items():
            headers[key] = value
        if not 'Authorization' in headers:
            headers['Authorization'] = f'{self.bearer} {self.get_access_token()}'
        response = requests.request(method, url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()

    def make_request_apikey(self, url, method='GET', data=None, headers=None):
        self.logger.info(f'requesting {url}')
        headers = headers or {}     
        if self.auth_headers_extra is not None:   
            for key,value in self.auth_headers_extra.items():
                headers[key] = value
        match str(self.auth_type).upper():
            case 'OAUTH2':
                if not 'Authorization' in headers:
                    headers['Authorization'] = f'{self.bearer} {self.get_access_token()}'
            # case 'APIKEY':
        if self.request_headers is not None:
            for key,value in self.request_headers.items():
                headers[key] = value
        response = requests.request(method, url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()

    def make_request(self, url, method='GET', data=None, headers=None):
        self.logger.info(f'requesting {url}')
        headers = headers or {}     
        if self.auth_headers_extra is not None:   
            for key,value in self.auth_headers_extra.items():
                headers[key] = value
        match str(self.auth_type).upper():
            case 'OAUTH2':
                if not 'Authorization' in headers:
                    headers['Authorization'] = f'{self.bearer} {self.get_access_token()}'
            # case 'APIKEY':
        if self.request_headers is not None:
            for key,value in self.request_headers.items():
                headers[key] = value
        response = requests.request(method, url, headers=headers, data=data)
        if self.config.get('raise_for_status',False):
            response.raise_for_status()
        return response.json()

    def extract(self, *args, **kwargs):
        data_api = self.make_request(
                    url=self.request_url,
                    method=self.request_method,
                    data=self.request_data,            
                )
                
        # list or object?
        df = pandas.read_json(
            data_api,
            **self.request_read_json_params,
            )
        self._data = df

class PipelineSourceFlatFile(PipelineSource):
    def __init__(self, config: dict, enviroment:CloudEnvironment, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
        self.env = enviroment

    def generate_file_path(self) -> str:
        """
        Generate file path based on instructions in config dictionary
        """
        
        # kinda stuck in here!!!
        self.env.download_flat_files(self.config)

        # Get the file name and extension
        file_name = self.config.get('file_name', 'data')
        file_ext = self.config.get('file_ext', 'csv')

        # Concatenate the base directory, file name, and extension to get the full file path
        file_path = os.path.join(f'{file_name}.{file_ext}')

        return file_path

    def extract(self):
        # Check if the file path is defined in the config
        file_paths = self.config.get('file_path', None)
        if file_paths is None:
            self.env.download_flat_files(self.config)
            if self.config.get('storage',{}).get('download_files',False):
                file_paths = os.listdir('borderliner_downloads')
            else:
                file_paths = self.env.data_buffers
        

        # Read the file(s) into a pandas DataFrame or list of DataFrames
        if isinstance(file_paths, str):
            if file_paths.endswith('.csv'):
                read_csv_params = self.config.get('read_csv_params', {})
                self._data = pandas.read_csv(file_paths, **read_csv_params)
            elif file_paths.endswith('.xlsx'):
                read_excel_params = self.config.get('read_excel_params', {})
                self._data = pandas.read_excel(file_paths, **read_excel_params)
            else:
                raise ValueError('File type not supported')
        elif isinstance(file_paths, list):
            self._data = []
            for file_path in file_paths:
                if isinstance(file_path, io.BytesIO):
                    file_path.seek(0) # reset the stream to the beginning
                    if self.config.get('extension','csv').upper() == 'CSV':
                        read_csv_params = self.config.get('read_csv_params', {})
                        df = pandas.read_csv(file_path, **read_csv_params)
                    elif self.config.get('extension','csv').upper() == 'XLSX':
                        read_excel_params = self.config.get('read_excel_params', {})
                        df = pandas.read_excel(file_path, **read_excel_params)
                    else:
                        raise ValueError('File type not supported')
                    self._data.append(df)
                elif isinstance(file_path,str):
                    if file_path.endswith('.csv'):
                        read_csv_params = self.config.get('read_csv_params', {})
                        df = pandas.read_csv(file_path, **read_csv_params)
                    elif file_path.endswith('.xlsx'):
                        read_excel_params = self.config.get('read_excel_params', {})
                        df = pandas.read_excel(file_path, **read_excel_params)
                    else:
                        raise ValueError('File type not supported')
                    self._data.append(df)
        else:
            raise ValueError('Invalid file path(s) in config')

