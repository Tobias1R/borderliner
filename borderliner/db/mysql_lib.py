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
from . import conn_abstract
from pandas._libs.lib import infer_dtype
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import warnings
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.sql import text
from sqlalchemy import MetaData
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Engine
import MySQLdb


class MySqlBackend(conn_abstract.DatabaseBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_name = 'MySQL'
        self.alchemy_engine_flag = 'mysql+mysqlconnector'
        self.expected_connection_args = [
            'host',
            'user',
            'password',
            'database'
        ]
        self.meta = MetaData(bind=self.engine)
        self.database_module = MySQLdb
        self.driver_signature = ''
        self.ssl_mode = False

    def table_exists(self, table_name: str, schema: str):
        try:
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            query = f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'"
            cursor.execute(query)
            exists = cursor.fetchone()[0] == table_name
            cursor.close()
            conn.close()
            return exists
        except Exception as e:
            return False

    def inspect_table(self, schema: str, table_name: str):
        data = []
        query = f"SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'"
        conn = self.get_connection().raw_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        for row in result:
            data.append(row)
        cursor.close()
        conn.close()
        return data

    def get_connection(self, *args, **kwargs):
        kwargs['ssl'] = False
        return self.get_engine(*args, **kwargs)

    def get_engine(self, *args, **kwargs) -> Engine:
        if isinstance(self.engine, Engine):
            return self.engine
        self.engine = create_engine(self.uri)
        return self.engine
    
    def bulk_insert(self, active_connection: Engine,
                    data: pd.DataFrame,
                    schema: str,
                    table_name: str):
        table = f"{schema}.{table_name}"
        columns = ", ".join(data.columns)
        values = [self.extract_values(x) for x in data.values]
        #values = [dict(row) for _, row in data.iterrows()]
        stmt = f"INSERT INTO {table} ({columns}) VALUES ({', '.join(f'%s' for col in data.columns)})"
        
        try:
            conn = active_connection.raw_connection()
            cursor = conn.cursor()
            # print(stmt)
            # print(values[0])
            cursor.executemany(stmt, values)
            cursor.close()
            conn.commit()
        except exc.SQLAlchemyError as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def insert_on_conflict(self, 
            active_connection: Engine, 
            df: pd.DataFrame, 
            schema: str, 
            table_name: str, 
            if_exists='append', 
            conflict_key=None, 
            conflict_action=None):
        # Create the target table object
        target_table = Table(
            table_name,
            self.meta,
            schema=schema,
            autoload=True,
            autoload_with=active_connection
        )

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
            update_clause = ', '.join([f"{col}=VALUES({col})" for col in update_cols])
            conflict_behavior = f"ON DUPLICATE KEY UPDATE {update_clause}"
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
        total_rows_table_before = self.count_records(cursor,f'{schema}.{table_name}')
        self.logger.info(f'ROWS IN TARGET: {total_rows_table_before}')
        if len(df) > max_rows:
            for i in range(0, num_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                # Generate the SQL statement and execute it
                values_str = ', '.join(['%s' for x in chunk.columns])
                column_str = ', '.join(chunk.columns)
                values = [self.extract_values(x) for x in chunk.values]
                merge_statement = f"""INSERT INTO {schema}.{table_name} ({column_str}) 
                            VALUES ({values_str}) 
                            ON DUPLICATE KEY UPDATE {update_clause};"""
                cursor.executemany(merge_statement,values)
                inserted_rows += cursor.rowcount
                #updated_rows += len(chunk)-cursor.rowcount
        else:
            values_str = ', '.join(['%s' for x in df.columns])
            column_str = ', '.join(df.columns)
            values = [self.extract_values(x) for x in df.values]
            merge_statement = f"""INSERT INTO {schema}.{table_name} ({column_str}) 
                        VALUES ({values_str}) 
                        ON DUPLICATE KEY UPDATE {update_clause};"""
            cursor.executemany(merge_statement, values)
            inserted_rows = cursor.rowcount
        cursor.execute('COMMIT;')
        total_rows_table_after = self.count_records(cursor,f'{schema}.{table_name}')
        cursor.close()
        connection.close()
        inserted_rows_temp = total_rows_table_before - total_rows_table_after
        self.execution_metrics['inserted_rows'] += inserted_rows_temp
        if inserted_rows_temp == 0:
            updated_rows = inserted_rows
        self.execution_metrics['updated_rows'] += updated_rows

Connection = MySqlBackend