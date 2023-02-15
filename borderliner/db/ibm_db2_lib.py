from . import conn_abstract
from pandas._libs.lib import infer_dtype
from psycopg2.extras import execute_values
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import warnings
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.sql import text
from psycopg2 import Timestamp
from sqlalchemy import MetaData

class IbmDB2Backend(conn_abstract.DatabaseBackend):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.interface_name = 'DB2'
        self.alchemy_engine_flag = 'iaccess+pyodbc'
        self.expected_connection_args = [
                'host',
                'user',
                'password'
            ]
        self.database = 'dummy?DBQ=DEFAULT_SCHEMA'
        self.meta = MetaData(bind=self.engine)

    
    def get_connection(self, *args, **kwargs):
        return self.get_engine(*args, **kwargs)
    
    def insert_on_conflict(
        self, 
        active_connection: Engine, 
        df: pd.DataFrame, 
        schema: str, 
        table_name: str, 
        if_exists='append', 
        conflict_key=None, 
        conflict_action=None
    ):
        """
        Insert data from a pandas DataFrame into a table, with optional handling of conflicts.
        
        Parameters:
        -----------
        active_connection: sqlalchemy.engine.Engine
            Active database connection.
        df: pandas.DataFrame
            Data to be inserted.
        schema: str
            Schema of the target table.
        table_name: str
            Name of the target table.
        if_exists: str
            How to handle existing data. Can be 'fail', 'replace', or 'append'.
        conflict_key: str or None
            Name of the column(s) to use as a conflict key.
        conflict_action: str or None
            How to handle conflicts. Can be 'ignore' or 'update'.
        
        Returns:
        --------
        None
        """
        # Create the target table object
        target_table = Table(table_name, self.meta, schema=schema, autoload=True, autoload_with=active_connection)
        
        # Determine the insert behavior based on the if_exists argument
        if if_exists == 'fail':
            insert_behavior = None
        elif if_exists == 'replace':
            insert_behavior = 'replace'
        else:
            insert_behavior = 'append'
        
        # Determine the conflict handling behavior based on the conflict_action argument
        if conflict_action == 'ignore':
            conflict_behavior = 'DO NOTHING'
        elif conflict_action == 'update':
            if conflict_key is None:
                raise ValueError("conflict_key must be specified when using 'update' conflict action")
            conflict_cols = conflict_key if isinstance(conflict_key, (list, tuple)) else (conflict_key,)
            update_cols = [col for col in df.columns if col not in conflict_cols]
            update_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            conflict_behavior = f"DO UPDATE SET {update_clause}"
        else:
            conflict_behavior = ''
        
        # Generate the SQL statement and execute it
        sql = target_table.insert().values(df.to_dict('records')).\
              on_conflict_do_nothing(index_elements=conflict_key)
        if conflict_behavior:
            sql = text(f"{sql.compile(compile_kwargs={'literal_binds': True})} {conflict_behavior}")
        result = active_connection.execute(sql)
        
        # Count the number of inserted and updated rows
        inserted_rows = result.rowcount
        updated_rows = result.context.result.rowcount if conflict_action == 'update' else 0
        
        self.execution_metrics['inserted_rows'] = inserted_rows
        self.execution_metrics['updated_rows'] = updated_rows
        

Connection = IbmDB2Backend