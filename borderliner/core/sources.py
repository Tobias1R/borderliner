import base64
import importlib
import io
import os
import re
import pandas
import logging
import sys
# APACHE ARROW
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import Table, Schema
import pyarrow as pa

import imaplib
import email
import email.header
import email.utils
import email.mime.multipart
import email.mime.text
import email.mime.base
import email.mime.application
import email.mime.image
import email.mime.audio
import email.mime.message
import email.mime.audio
import email.mime.image

from borderliner.db.conn_abstract import DatabaseBackend
from borderliner.cloud import CloudEnvironment

# logging
from borderliner.core.logs import get_logger
logger = get_logger()

def download_email_attachments(email, password, server, port, folder, protocol, attachments):
    import imaplib
    import email
    import os
    import sys
    import traceback
    import re
    import datetime
    import email.header
    import email.utils
    import email.mime.multipart
    import email.mime.text
    import email.mime.base
    import email.mime.application
    import email.mime.image
    import email.mime.audio
    import email.mime.message
    import email.mime.audio
    import email.mime.image
    
    if protocol == 'IMAP':
        mail = imaplib.IMAP4_SSL(server, port)
    elif protocol == 'POP3':
        mail = imaplib.POP3_SSL(server, port)
    else:
        raise ValueError('Invalid protocol')
    mail.login(email, password)
    mail.select(folder)
    typ, data = mail.search(None, 'ALL')
    for num in data[0].split():
        typ, data = mail.fetch(num, '(RFC822)')
        msg = email.message_from_bytes(data[0][1])
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            filename = part.get_filename()
            if filename is not None:
                if filename.lower() in attachments:
                    if not os.path.exists(attachments[filename.lower()]):
                        fp = open(attachments[filename.lower()], 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
    mail.close()
    mail.logout()

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
        self.dynamic_params = {}
    
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
    

class PipelineSourceEMail(PipelineSource):
    def __init__(self, config: dict,*args,**kwargs) -> None:
        super().__init__(config,*args,**kwargs)
        self.email = config['email']
        self.password = config['password']
        self.server = config['server']
        self.port = config['port']
        self.folder = config['folder']
        self.protocol = config['protocol']
        self.attachments = []
        self.search_mail = config.get('search_mail','ALL')
        self.connection = None
        self.messages = None
        self.return_data = 'attachments'
        self.kwargs = kwargs
        # An object to handle email operations
        self.mail = None
        self.configure()
    
    def configure(self):
        pass

    def extract(self):
        self.logger.info('Extracting data from email')
        self.mail = imaplib.IMAP4_SSL(self.server, self.port)
        self.mail.login(self.email, self.password)
        self.mail.select(self.folder)
        typ, data = self.mail.search(None, self.search_mail)
        # create a dataframe to hold the attachment name, the attachment as base64, and the timestamp
        dataframe = pandas.DataFrame(columns=['attachment_name', 'attachment', 'timestamp'])
        search_subject_pattern = self.config.get('search_subject_pattern',None)
        save_path = self.config.get('save_path',None)
        self.messages = []
        for num in data[0].split():
            typ, data = self.mail.fetch(num, '(RFC822)')
            msg = email.message_from_bytes(data[0][1])
            if search_subject_pattern:
                if re.search(search_subject_pattern,msg['Subject']):
                    self.messages.append(msg)
                    for part in msg.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue
                        filename = part.get_filename().replace(' ','_')
                        # add path to filename
                        if save_path:
                            # create the folder if it does not exist
                            if not os.path.exists(save_path):
                                os.makedirs(save_path)
                            filename = os.path.join(save_path,filename)

                        if filename is not None:
                            self.logger.info(f'Extracting attachment {filename}')
                            if not os.path.exists(filename):
                                fp = open(filename, 'wb')
                                fp.write(part.get_payload(decode=True))
                                fp.close()
                                attachment = part.get_payload(decode=True)
                                # convert to base64
                                attachment = base64.b64encode(attachment)
                                # populate dataframe
                                dataframe = dataframe.append({'attachment_name': filename,
                                                            'attachment': attachment,
                                                            'timestamp': msg['Date']}, ignore_index=True)
                                self.attachments.append(filename)
                                
                            
        self.mail.close()
        self.mail.logout()
        self.logger.info(f'Extracted {len(self.messages)} messages')
        self._data = dataframe
        return self._data
        

    

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
    
    def replace_env_vars(self,data):
        for key, value in data.items():
            if isinstance(value, dict):
                self.replace_env_vars(value)
            elif isinstance(value, str) and value.startswith("$ENV_"):
                env_var = value[5:]
                if env_var in os.environ:
                    data[key] = os.environ[env_var]
                else:
                    raise ValueError(f'Environment variable {value} not found')
        return data

    def configure(self):
        return self.configure_dynamic()
        
    
    def configure_dynamic(self):
        db_type = str(self.config['type']).upper()
        backend_class = self.config.get('backend_class', None) # get backend_class from config
        
        self.config = self.replace_env_vars(self.config)
        self.user = self.config.get('username', None)
        self.password = self.config.get('password', None)
        self.host = self.config.get('host', None)
        self.port = self.config.get('port', None)
        
        # check for external backend option
        if backend_class is None:
            # check for external backend class
            external_backend_class = self.config.get('external_backend_class', None)
            if external_backend_class is not None:
                backend_class = external_backend_class
        
        # dynamically import backend class based on backend_class variable
        if backend_class is not None:
            if isinstance(backend_class, str):
                backend_module_path = self.config.get('backend_module_path', '')
                backend_module = self.config.get('backend_module','db_backend')
                sys.path.append(backend_module_path)
                backend_module = importlib.import_module(f"{backend_module}")
                backend_class = getattr(backend_module, backend_class)
        else:
            # default to selecting backend based on db_type
            if db_type == 'POSTGRES':
                from borderliner.db.postgres_lib import PostgresBackend
                backend_class = PostgresBackend
            elif db_type == 'REDSHIFT':
                from borderliner.db.redshift_lib import RedshiftBackend
                backend_class = RedshiftBackend
            elif db_type == 'IBMDB2':
                from borderliner.db.ibm_db2_lib import IbmDB2Backend
                backend_class = IbmDB2Backend
        
        self.backend = backend_class(
            host=self.host,
            database=self.config['database'],
            user=self.user,
            password=self.password,
            port=self.port,
            **self.config.get('backend_options', {}) # pass backend options from config
        )
        self.queries = self.config['queries']
        self.logger.info(f'backend for {db_type} loaded')
        self.engine = self.backend.get_engine()
        self.connection = self.backend.get_connection()

    def populate_deltas(self):
        pass

    def df_to_parquet(self, df:pandas.DataFrame,filename):
        if self.control_columns_function:
            df = self.control_columns_function(df) 
        df.to_parquet(
                    filename,
                    index=False
                )
        self.metrics['total_rows'] += len(df)
        self.csv_chunks_files.append(filename)

    def populate_iteration_listARROW(self):
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

            if self.config.get('use_pyarrow', False):
                if self.chunk_size <= 0:
                    self.chunk_size = 100000
                # create a dataset from the SQL table
                self.logger.info(f'Extracting using Apache Arrow: {self.chunk_size}')
                connection = self.backend.get_engine().raw_connection()
                data = ds.dataset(
                    f'{self.backend.uri}::{query}',
                    schema=pa.schema(connection.cursor().description),
                    format='arrow',
                    partitioning='hive'
                )               
                
                for batch in data.to_batches(max_chunksize=self.chunk_size):
                    # convert the batch to a pandas dataframe
                    df = batch.to_pandas()

                    # apply any necessary transformations to the dataframe
                    if self.control_columns_function:
                        df = self.control_columns_function(df)

                    # write the data to a Parquet file
                    filename = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                    self.df_to_parquet(df,filename)   
                    

                    # increment the slice index
                    slice_index += 1
            else:
                data = pandas.read_sql_query(query, self.engine)
                self.metrics['total_rows'] += len(data)

                if self.kwargs.get('dump_data_csv', False):
                    filename_csv = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.csv'
                    filename_parquet = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                    if self.control_columns_function:
                        data = self.control_columns_function(data)
                    if self.kwargs.get('dump_data_csv', False) == 'CSV':
                        filename = filename_csv
                        data.to_csv(filename_csv, index=False)
                    else:
                        filename = filename_parquet
                        data.to_parquet(filename_parquet, index=False)
                    self.csv_chunks_files.append(filename)
                else:
                    self._data.append(data)

                slice_index += 1

    def populate_iteration_list(self):
        self.logger.info('Populating iteration list')
        df = pandas.read_sql_query(
            self.queries['iterate'],
            self.engine
        )
        self.logger.info(f'Extract by iteration: {len(df)} items')

        
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
            if self.chunk_size <= 0:
                self.chunk_size = 100000
            try:
                if self.pipeline_config.debug_query:
                    print(query)
            except:
                pass
            data = pandas.read_sql_query(
                    query,
                    self.engine,
                    chunksize=self.chunk_size)
            
            
            #self.metrics['total_rows'] += len(data)
            if self.kwargs.get('dump_data_csv',False):
                filename_parquet = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                if isinstance(data,pandas.DataFrame):
                    df = data
                    if len(df) > 0:
                        self.df_to_parquet(df,filename_parquet)
                    
                else:
                    iter_slice = 1
                    for df in data:
                        filename = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}_{str(iter_slice).zfill(5)}.parquet'
                        if len(df) > 0:
                            self.df_to_parquet(df,filename)                        
                            iter_slice += 1

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
            

            if self.config.get('use_pyarrow',False):
                # create a dataset from the SQL table
                self.logger.info(f'Extracting using Apache arrow: {self.chunk_size}')
                query = self.get_query('extract')
                data = ds.dataset(
                    f'{self.backend.uri}::{query}',
                    format='sql',
                    batch_size=self.chunk_size
                )

                # create a scanner to read the data in batches
                scanner = data.scanner()
            else:
                self.logger.info(f'Extracting chunk size: {self.chunk_size}')
                data = pandas.read_sql_query(
                    self.get_query('extract'),
                    self.engine,
                    chunksize=self.chunk_size)
                
                slice_index = 1
            if self.kwargs.get('dump_data_csv',False):
                if self.config.get('use_pyarrow',False):
                    # write the dataset to Parquet files in chunks                    
                    for batch in data.to_batches():                        
                        filename = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                        df = batch.to_pandas()                                        
                        self.df_to_parquet(df,filename)                        
                        slice_index += 1                        
                else:
                    for df in data:                        
                        filename = f'{self.pipeline_name}_slice_{str(slice_index).zfill(5)}.parquet'
                        self.df_to_parquet(df,filename)                        
                        slice_index += 1
                        
            else:
                self._data = data
        else:
            print(self.engine)
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

    def get_dynamic_params(self)->dict:
        '''
        Returns a dict with dynamic params.
        You can override this method to add more params.
        Returns: {param_name:param_value, ...}
        '''
        return self.dynamic_params  
    
    def get_query(self,query:str='extract'):
        from sqlalchemy import text

        
        if query in self.queries:
            key_params = str(query)+'_params'
            params = self.queries.get(key_params,{})
            
            # merge with dynamic params
            params = {**params,**self.get_dynamic_params()}
            if params:
                
                if '%' in self.queries[query]:
                    return text(self.queries[query].format(
                        **params
                    ))
                return self.queries[query].format(
                    **params
                )
            else:
                if '%' in self.queries[query]:
                    return text(self.queries[query])
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
        
        if self.config.get('parse_files',True):
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
        else:
            self.logger.info('not parsing files')
            self._data = file_paths

