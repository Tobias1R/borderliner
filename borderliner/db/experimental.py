# sqlalchemy
import psycopg2
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
# SqlAlchemy Dialects
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import oracle, firebird, mssql, mysql, postgresql, sqlite

class DatabaseBackend:
    def __init__(self, *args, **kwargs) -> None:
        self.dialect = None
        self.database_module = psycopg2
        self.alchemy_engine_flag = 'psycopg2'
        self.driver_signature = ''
        self.user:str = ''
        self.database:str = ''
        self.password:str = ''
        self.host:str = ''
        self.port:str = None
        self.count = 0
        self.total_time = 0.0
        self.ssl_mode = 'prefer'        
        self.session = None
        self.engine = None
        self.execution_metrics = {
            'inserted_rows':0,
            'updated_rows':0,
            'deleted_rows':0,
            'processed_rows':0,
            }
        #self.set_engine()
        self.create_table = False
        # Define if the count of rows should be used for metrics
        self.use_count_for_metrics = False