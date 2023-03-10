
import importlib
import os
import pandas
import logging
import sys
from sqlalchemy import MetaData, Table, Column, String, TIMESTAMP, BIGINT
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import types
from borderliner.db.conn_abstract import DatabaseBackend
from borderliner.db.postgres_lib import PostgresBackend
from borderliner.db.redshift_lib import RedshiftBackend
from borderliner.db.ibm_db2_lib import IbmDB2Backend
from borderliner.db.dbutils import get_column_type
# logging
from borderliner.core.logs import get_logger
logger = get_logger()

class PipelineTarget:
    def __init__(self,config,*args,**kwargs) -> None:
        self.logger = logger
        self.kwargs = kwargs
        self.pipeline_pid = self.kwargs.get('pipeline_pid',0)
        self.dump_data_csv = kwargs.get('dump_data_csv',False)
        self.csv_chunks_files = kwargs.get('csv_chunks_files',[])
        self.control_columns = kwargs.get('control_columns',False)
        self.control_columns_names = kwargs.get('control_columns_names',{})
        self.pipeline_config = config
        self.config = config.target
        self._data:pandas.DataFrame|list = []
        self.chunk_size = -1
        self.metrics:dict = {
            'total_rows':0,
            'inserted_rows':0,
            'updated_rows':0,
            'deleted_rows':0,
            'processed_rows':0
        }
        self.database_module = 'psycopg2'
        self.alchemy_engine_flag = 'psycopg2'
        self.driver_signature = ''
        self.backend:DatabaseBackend = None
        self.user:str = ''
        self.database:str = ''
        self.password:str = ''
        self.host:str = ''
        self.port:str = None
        
        self.count = 0
        self.total_time = 0.0

        self.engine = None
        self.connection = None

        self.iteration_list = []
        self.deltas = {}
        self.primary_key = ()

        self.source_schema = []

        self.configure()

    def replace_env_vars(self,data):
        for key, value in data.items():
            if isinstance(value, dict):
                self.replace_env_vars(value)
            elif isinstance(value, str) and value.startswith("$ENV_"):
                env_var = value[5:]
                if env_var in os.environ:
                    data[key] = os.environ[env_var]
                else:
                    raise ValueError(f"Environment variable {env_var} not found")
        return data
    
    def configure(self):
        return self.configure_dynamic()
        db_type = str(self.config['type']).upper()
        
        self.config = self.replace_env_vars(self.config)
        self.user = self.config.get('username',None)
        self.password = self.config.get('password',None)
        self.host = self.config.get('host',None)
        self.port = self.config.get('port',None)
        
        match db_type:
            case 'POSTGRES':
                self.backend = PostgresBackend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
            case 'REDSHIFT':
                self.backend = RedshiftBackend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    staging_schema=self.config.get('staging_schema','staging'),
                    staging_table=self.config.get('staging_table',None)
                )
            case 'IBMDB2':
                self.backend = IbmDB2Backend(
                    host=self.host,
                    database=self.config['database'],
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
        self.logger.info(f'backend for {db_type} loaded')
        self.engine = self.backend.get_engine()
        self.connection = self.backend.get_connection()
    
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
                backend_module = importlib.import_module(f"{backend_module}.{backend_class}")
                backend_class = getattr(backend_module, backend_class)
        else:
            # default to selecting backend based on db_type
            if db_type == 'POSTGRES':
                backend_class = PostgresBackend
            elif db_type == 'REDSHIFT':
                backend_class = RedshiftBackend
            elif db_type == 'IBMDB2':
                backend_class = IbmDB2Backend
            elif db_type == 'MYSQL':
                from borderliner.db.mysql_lib import MySqlBackend
                backend_class = MySqlBackend
        
        self.backend = backend_class(
            host=self.host,
            database=self.config['database'],
            user=self.user,
            password=self.password,
            port=self.port,
            **self.config.get('backend_options', {}) # pass backend options from config
        )
        
        self.logger.info(f'backend for {db_type} loaded')
        self.engine = self.backend.get_engine()
        self.connection = self.backend.get_connection()



    def __str__(self) -> str:
        return str(self.config['type']).upper()
    
    def load(self,data:pandas.DataFrame|list):
        if self.dump_data_csv:
            self.csv_chunks_files = list(set(self.csv_chunks_files))
            self.csv_chunks_files.sort()
            for filename in self.csv_chunks_files:
                self.logger.info(f'reading parquet {filename}')
                df = pandas.read_parquet(filename)
                self._data=df
                self.save_data()
        else:
            self._data=data
            self.save_data()
        if self.backend:
            self.metrics = self.backend.execution_metrics

    def save_data(self):
        pass

class PipelineTargetDatabase(PipelineTarget):
    def __init__(self, config: dict,*args,**kwargs) -> None:
        super().__init__(config,*args,**kwargs)

    def use_staging_table(self)->bool|str:
        return self.config.get('staging_schema',False)
    
    def create_table(self, source_schema: Table):
        """
        Creates a reflected table from source table.
        """
        self.logger.info('Creating table from source schema.')
        

        def _create_table(target_schema,target_table):
            source_schema.name = target_table
            # Create a new table with the same structure
            target_metadata = MetaData(schema=target_schema)
            columns_dict = {}
            #pk_cols = [col for col in getattr(source_schema, 'primary_key', [])]
            pk_cols = [] #[col for col in source_schema.primary_key]
            for col in source_schema.primary_key:
                if col.name in self.config.get('target_table_definition',{}).keys():
                    col_cnf = self.config.get('target_table_definition',{})[col.name]
                    col_type = get_column_type(col_cnf.get('type', 'VARCHAR'))()
                    if col_cnf.get('size'):
                        col_type.length = col_cnf['size']
                    col = col.copy()
                    col.type = col_type
                    #col = Column(col.name, String(255),primary_key=True)
                    
                pk_cols.append(col)
            
            
            for col in source_schema.columns:
                print(col,str(col.type))
                for colname, col_cnf in self.config.get('target_table_definition', {}).items():
                    if col.name == colname:
                        col_type = get_column_type(col_cnf.get('type', 'VARCHAR'))()
                        if col_cnf.get('size'):
                            col_type.length = col_cnf['size']
                        if col_cnf.get('precision'):
                            col_type.precision = col_cnf['precision']
                        if col_cnf.get('scale'):
                            col_type.scale = col_cnf['scale']
                        columns_dict[col.name] = col.copy()
                        columns_dict[col.name].type = col_type
                if not col.name in columns_dict.keys():
                    col2 = col.copy()
                    col_type = get_column_type(str(col.type))()
                    col2.type = col_type
                    columns_dict[col.name] = col2
            
            # control columns to dict
            if self.control_columns:
                data_md5_label = self.control_columns_names.get('data_md5_label','brdr_data_md5')
                extract_date_label = self.control_columns_names.get('extract_date_label','brdr_extract_date')
                columns_dict[data_md5_label] = Column(data_md5_label,String(32))
                columns_dict[extract_date_label] = Column(extract_date_label,BIGINT)        
                    

            columns = list(columns_dict.values())
            #print(columns)
            table_args = []
            if pk_cols:
                
                table_args.append(PrimaryKeyConstraint(*pk_cols))
                target_table_object = Table(target_table, target_metadata, *columns, *table_args)
            else:
                
                target_table_object = Table(target_table, target_metadata, *columns)
            # Create the table in the target database
            with self.engine.begin() as conn:
                target_table_object.create(conn)
            self.logger.info('Created table: %s.%s', target_schema, target_table)

        target_schema = self.config.get('schema')
        target_table = self.config.get('table')
        _create_table(target_schema,target_table)
        if self.use_staging_table():
            target_schema = self.config.get('staging_schema')
            target_table = self.config.get('staging_table')
            _create_table(target_schema,target_table)

    def _do_upsert(self):
        insmethod='UPSERT'
        if isinstance(self._data,pandas.DataFrame):
            
            total_rows = len(self._data)
            self.logger.info(f'Insertion Method: {insmethod} for {total_rows} rows')
            self.backend.insert_on_conflict(
                self.engine,
                self._data,
                self.config['schema'],
                self.config['table'],
                if_exists='append',
                conflict_action=self.config.get('conflict_action',None),
                conflict_key=self.config.get('conflict_key',None)
            )
        if isinstance(self._data,list):
            for df in self._data:
                
                total_rows = len(df)
                self.logger.info(f'Insertion Method: {insmethod} for {total_rows} rows')
                self.backend.insert_on_conflict(
                    self.engine,
                    df,
                    self.config['schema'],
                    self.config['table'],
                    if_exists='append',
                    conflict_action='update',
                    conflict_key=self.config['conflict_key']
                )

    def _do_bulk_insert(self):
        insmethod='BULK_INSERT'
        if isinstance(self._data,pandas.DataFrame):
            
            total_rows = len(self._data)
            self.logger.info(f'Insertion Method: {insmethod} for {total_rows} rows')
            self.backend.bulk_insert(
                self.engine,
                self._data,
                self.config['schema'],
                self.config['table']
            )
        if isinstance(self._data,list):
            for df in self._data:
                
                total_rows = len(df)
                self.logger.info(f'Insertion Method: {insmethod} for {total_rows} rows')
                self.backend.bulk_insert(
                    self.engine,
                    df,
                    self.config['schema'],
                    self.config['table']
                )

    def save_data(self):
        insmethod = self.config.get('insertion_method','UPSERT')
        match insmethod.upper():
            case 'UPSERT':
                self._do_upsert()
            case 'BULK_INSERT':
                self._do_bulk_insert()
        
        

class PipelineTargetApi(PipelineTarget):
    pass

class PipelineTargetFlatFile(PipelineTarget):
    
    def configure(self):
        self.logger.info('Target flat file configuration')
    
    def get_filename(self):
        file_extension = self.config.get('extension','CSV')
        filename = 'filename'
        cnf_filename = self.config.get('filename','default')
        if '{PID}' in cnf_filename:
            filename = str(cnf_filename).replace('{PID}',str(self.pipeline_pid))
        elif '{YYYYMMDD}' in cnf_filename:
            filename = str(cnf_filename).replace('{YYYYMMDD}',str(self.pipeline_pid)[:8])
        else:
            filename = cnf_filename
        return f'{filename}.{file_extension}'
    
    def _save_to_csv(self):
        filename = self.get_filename()
        if isinstance(self._data,pandas.DataFrame):
            if os.path.exists(filename):
                # file exists, append without header
                header = False
                mode = 'a'
            else:
                # file does not exist, create with header
                header = self.config.get('header', True)
                mode = 'w'
            self._data.to_csv(
                filename,
                sep=self.config.get('separator',','),
                header=header,
                index=self.config.get('index',False),
                mode=mode
            )
        if isinstance(self._data,list):
            for i, df in enumerate(self._data):
                header = True if i == 0 else False

                df.to_csv(
                    filename,
                    sep=self.config.get('separator',','),
                    header=header,
                    index=self.config.get('index',False),
                    mode='a'
                )
        self.logger.info(f'{filename} saved')

    def save_data(self):
        file_extension = self.config.get('extension','CSV')
        if str(file_extension).upper() == 'CSV':
            self._save_to_csv()
        return super().save_data()