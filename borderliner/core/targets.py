
import os
import pandas
import logging
import sys

from borderliner.db.conn_abstract import DatabaseBackend
from borderliner.db.postgres_lib import PostgresBackend
from borderliner.db.redshift_lib import RedshiftBackend
from borderliner.db.ibm_db2_lib import IbmDB2Backend
# logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()

class PipelineTarget:
    def __init__(self,config:dict,*args,**kwargs) -> None:
        self.logger = logger
        self.kwargs = kwargs
        self.pipeline_pid = self.kwargs.get('pipeline_pid',0)
        self.dump_data_csv = kwargs.get('dump_data_csv',False)
        self.csv_chunks_files = kwargs.get('csv_chunks_files',[])
        self.config = config
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
        self.config = self.replace_env_vars(self.config)
        self.user = self.config['username']
        self.password = self.config['password']
        self.host = self.config['host']
        self.port = self.config['port']
        match str(self.config['type']).upper():
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
        
        self.engine = self.backend.get_engine()
        self.connection = self.backend.get_connection()
    
    def __str__(self) -> str:
        return str(self.config['type']).upper()
    
    def load(self,data:pandas.DataFrame|list):
        if self.dump_data_csv:
            self.csv_chunks_files = list(set(self.csv_chunks_files))
            for filename in self.csv_chunks_files:
                self.logger.info(f'reading csv {filename}')
                df = pandas.read_csv(filename)
                self._data=df
                self.save_data()
        else:
            self._data=data
            self.save_data()
        self.metrics = self.backend.execution_metrics

    def save_data(self):
        pass

class PipelineTargetDatabase(PipelineTarget):
    def __init__(self, config: dict,*args,**kwargs) -> None:
        super().__init__(config,*args,**kwargs)

    def save_data(self):
        if isinstance(self._data,pandas.DataFrame):
            insmethod = self.config.get('insertion_method','UPSERT')
            total_rows = len(self._data)
            self.logger.info(f'Insertion Method: {insmethod} for {total_rows} rows')
            self.backend.insert_on_conflict(
                self.engine,
                self._data,
                self.config['schema'],
                self.config['table'],
                if_exists='append',
                conflict_action='update',
                conflict_key=self.config['conflict_key']
            )
        if isinstance(self._data,list):
            for df in self._data:
                insmethod = self.config.get('insertion_method','UPSERT')
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
        
        

class PipelineTargetApi(PipelineTarget):
    pass

class PipelineTargetFlatFile(PipelineTarget):
    pass