import pandas
import logging
import sys

from borderliner.db.conn_abstract import DatabaseBackend
from borderliner.db.postgres_lib import PostgresBackend
from borderliner.db.redshift_lib import RedshiftBackend
# logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()

class PipelineSource:
    def __init__(self,config:dict,*args,**kwargs) -> None:
        self.kwargs = kwargs
        self.pipeline_pid = self.kwargs.get('pipeline_pid',0)
        self.csv_chunks_files = []
        self.logger = logger
        self.config = config
        self._data:pandas.DataFrame|list = None
        self.chunk_size = -1
        self.metrics:dict = {
            'total_rows':0,
            'processed_rows':0
        }
    
    def extract(self):
        pass

    def __str__(self) -> str:
        return str(self.config)

    @property
    def data(self):
        source_empty = True
        if isinstance(self._data,list):
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
        
        self.count = 0
        self.total_time = 0.0

        self.engine = None
        self.connection = None

        

        self.iteration_list = []
        self.deltas = {}
        self.primary_key = ()

        self.configure()
    
    def configure(self):
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
            if self.kwargs.get('dump_data_csv',False):
                filename = f'slice_{str(slice_index).zfill(5)}_{self.pipeline_pid}.csv'                
                data.to_csv(
                    filename,
                    header=True,
                    index=False
                )
                self.csv_chunks_files.append(filename)
                self._data.append(data)
            else:
                self._data.append(data)
            slice_index += 1
        

    def extract_by_iteration(self):
        self.populate_iteration_list()

    def extract(self):
        if 'iterate' in self.queries:
            self.extract_by_iteration()
            return
        if self.chunk_size > 0: 
            data = pandas.read_sql_query(
                self.get_query('extract'),
                self.engine,
                chunksize=self.chunk_size)
            slice_index = 1
            for df in data:
                if self.kwargs.get('dump_data_csv',False):
                    filename = f'slice_{str(slice_index).zfill(5)}_{self.pipeline_pid}.csv'                
                    df.to_csv(
                        filename,
                        header=True,
                        index=False
                    )
                    self.csv_chunks_files.append(filename)
                    slice_index += 1
            self._data = data
        else:
            data = pandas.read_sql_query(
                self.get_query('extract'),
                self.engine)
            if self.kwargs.get('dump_data_csv',False):
                filename = f'slice_FULL_{self.pipeline_pid}.csv'                
                data.to_csv(
                    filename,
                    header=True,
                    index=False
                )
                self.csv_chunks_files.append(filename)
            self._data = data
            

    def get_query(self,query:str='extract'):
        print(self.queries)
        if query in self.queries:
            key_params = str(query)+'_params'
            if key_params in self.queries:
                return self.queries[query].format(
                    **self.queries[key_params]
                )
            else:
                return self.queries[query]
        
        
        raise Exception('Query not found.')





class PipelineSourceApi(PipelineSource):
    pass

class PipelineSourceFlatFile(PipelineSource):
    pass
