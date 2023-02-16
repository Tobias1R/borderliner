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
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import ibm_db
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
class IbmDB2Backend(conn_abstract.DatabaseBackend):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.interface_name = 'DB2'
        self.alchemy_engine_flag = 'db2+ibm_db'
        self.expected_connection_args = [
                'host',
                'user',
                'password'
            ]
        #self.database = 'dummy?DBQ=DEFAULT_SCHEMA'
        self.meta = MetaData(bind=self.engine)
        self.database_module = ibm_db
        self.driver_signature = '{IBM i Access ODBC Driver 64-bit}'
        self.ssl_mode = False

    
    def get_connection(self, *args, **kwargs):
        kwargs['ssl'] = False
        return self.get_engine(*args, **kwargs)
    
    def get_engine(self,*args,**kwargs)->Engine:
        if isinstance(self.engine,Engine):
            return self.engine
        self.engine = create_engine(self.uri)
        return self.engine
    
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
        # target_table = Table(
        #     table_name, 
        #     self.meta, 
        #     schema=schema, 
        #     autoload=True, 
        #     autoload_with=active_connection,
        #     ibm_db_ssl=False
        # )

        # Determine the insert behavior based on the if_exists argument
        if if_exists == 'fail':
            insert_behavior = None
        elif if_exists == 'replace':
            insert_behavior = 'replace'
        else:
            insert_behavior = 'append'

        # Determine the conflict handling behavior based on the conflict_action argument
        if conflict_action == 'ignore':
            conflict_behavior = ''
        elif conflict_action == 'update':
            if conflict_key is None:
                raise ValueError("conflict_key must be specified when using 'update' conflict action")
            conflict_cols = conflict_key if isinstance(conflict_key, (list, tuple)) else (conflict_key,)
            update_cols = [col for col in df.columns if col not in conflict_cols]
            update_clause = ', '.join([f"{col}=src.{col}" for col in update_cols])
            conflict_behavior = f"WHEN MATCHED THEN UPDATE SET {update_clause}"

            key_cols = conflict_key
            if isinstance(conflict_key,list):
                key_cols = ', '.join([col for col in conflict_key])
        else:
            conflict_behavior = ''

        inserted_rows = 0
        updated_rows = 0
        max_rows = 1000
        num_rows = len(df)
        chunk_size = max_rows
        connection = active_connection.raw_connection()
        cursor = connection.cursor()
        if len(df) > max_rows:
            for i in range(0, num_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                # Generate the SQL statement and execute it
                merge_statement = f"""MERGE INTO {schema}.{table_name} AS tgt
                        USING (VALUES {', '.join([str(tuple(x)) for x in chunk.values])}
                            )
                            AS src ({', '.join(chunk.columns)})
                        ON {' AND '.join([f'tgt.{col}=src.{col}' for col in conflict_cols])}
                        {conflict_behavior}
                        WHEN NOT MATCHED THEN INSERT ({', '.join(chunk.columns)})
                            VALUES ({', '.join(['src.'+col for col in chunk.columns])});"""
                cursor.execute(
                            merge_statement, 
                            df.to_dict("records"))
                inserted_rows += cursor.rowcount
                updated_rows += len(chunk)-cursor.rowcount
        else:
            merge_statement = f"""MERGE INTO {schema}.{table_name} AS tgt
                        USING (VALUES {', '.join([str(tuple(x)) for x in df.values])}
                            )
                            AS src ({', '.join(df.columns)})
                        ON {' AND '.join([f'tgt.{col}=src.{col}' for col in conflict_cols])}
                        {conflict_behavior}
                        WHEN NOT MATCHED THEN INSERT ({', '.join(df.columns)})
                            VALUES ({', '.join(['src.'+col for col in df.columns])});"""
            cursor.execute(
                        merge_statement, 
                        df.to_dict("records"))
            inserted_rows = cursor.rowcount
            updated_rows = len(df)-cursor.rowcount
        # for index, row in df.iterrows():
            
        #     merge_statement = f"""MERGE INTO {schema}.{table_name} AS tgt
        #             USING (VALUES {tuple(row.values)}
        #                 )
        #                 AS src ({', '.join(df.columns)})
        #             ON {' AND '.join([f'tgt.{col}=src.{col}' for col in conflict_cols])}
        #             {conflict_behavior}
        #             WHEN NOT MATCHED THEN INSERT ({', '.join(df.columns)})
        #                 VALUES ({', '.join(['src.'+col for col in df.columns])});"""
        #     cursor.execute(
        #             merge_statement, 
        #             row.to_dict())
            
        #     if isinstance(conflict_key,(list,tuple)):
        #         values = ', '.join(["'"+str(v)+"'" for v in row[conflict_key].values])
        #     else:
        #         values = row[conflict_key].values
        #     # Count the number of inserted and updated rows
        #     if self.val_record_exists(cursor,
        #         f'{schema}.{table_name}',
        #         key_cols,
        #         values
        #         ):
        #         updated_rows += cursor.rowcount*-1
        #     else:
        #         inserted_rows += cursor.rowcount
             #result.context.result.rowcount if conflict_action == 'update' else 0
        #print(result.context.result)
        cursor.execute('COMMIT;')
        cursor.close()
        connection.close()
        self.execution_metrics['inserted_rows'] = inserted_rows
        self.execution_metrics['updated_rows'] = updated_rows
        

Connection = IbmDB2Backend