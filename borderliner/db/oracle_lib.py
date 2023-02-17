import cx_Oracle
from . import conn_abstract
from pandas._libs.lib import infer_dtype
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import warnings
import pandas
from sqlalchemy import Table
from sqlalchemy.sql import text
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class OracleBackend(conn_abstract.DatabaseBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_name = 'oracle'
        self.alchemy_engine_flag = 'oracle'
        self.expected_connection_args = [
                'user',
                'password',
                'dsn'
            ]
        self.ssl_mode = 'prefer'
        
    @staticmethod
    def _sql_type_name(col_type):
        from pandas.io.sql import _SQL_TYPES
        if col_type == 'timedelta64':
            warnings.warn("the 'timedelta' type is not supported, and will be "
                          "written as integer values (ns frequency) to the "
                          "database.", UserWarning, stacklevel=8)
            col_type = "integer"
        elif col_type == "datetime64":
            col_type = "timestamp"
        elif col_type == "empty":
            col_type = "varchar2(255)"
        elif col_type == "complex":
            raise ValueError('Complex datatypes not supported')
        if col_type not in _SQL_TYPES:
            col_type = "varchar2(255)"
        return _SQL_TYPES[col_type]
    
    def table_exists(self, table_name: str, schema: str):
        try:
            conn = self.engine.raw_connection()
            cur = conn.cursor()
            query = f"SELECT table_name FROM all_tables WHERE table_name = '{table_name}' AND owner = '{schema.upper()}'"
            cur.execute(query)
            exists = cur.fetchone()[0]
            cur.close()
            conn.close()
            return exists
        except cx_Oracle.Error as e:
            return False

    def column_exists_db(
        self,
        active_connection: cx_Oracle.Connection,
        table_name, 
        column_name, 
        dtype, 
        if_not_exists='append'):
        """
        Check if column exists on db.
        """
        q = f"SELECT count(*) FROM all_tab_cols " \
            f"where table_name = '{table_name.upper()}' and column_name = '{column_name.upper()}'"
        conn = active_connection
        try:          
            if conn.execute(q).fetchone()[0] == 1:
                return 0
            elif conn.execute(q).fetchone()[0] == 0 and if_not_exists == 'append':
                dt = OracleBackend._sql_type_name(dtype.__str__())
                qc = f"ALTER TABLE {table_name} ADD {column_name.upper()} {dt}"
                conn.execute(qc)
                return 0
            else:
                print(conn.execute(q).fetchone())
                # TODO: the table wasn't altered.
                exit(1)
        except Exception as e:
            print('COL EXISTS EXCEPTION',e)
            raise e

    def insert_on_conflict(
    self, 
    active_connection:Engine,
    df:pandas.DataFrame, 
    schema,
    table_name,
    if_exists='append',
    conflict_key=None,
    conflict_action=None):
        """
        Process method to insert dataframes in database target.
        """
        try:
            connection = active_connection.raw_connection()
            cursor = connection.cursor()
            self.execution_metrics['processed_rows'] += len(df)
            if if_exists == 'append':
                for column in df.columns:
                    res = self.column_exists_db(
                        active_connection,
                        table_name, 
                        column, 
                        infer_dtype(df[column]))
                
                col_names = ','.join(str(e) for e in df.columns)
                data = [tuple(x) for x in df.values]
                
                if conflict_key != None:                    
                    conflict_set = ','.join(f"{col_name}=EXCLUDED.{col_name}" for col_name in df.columns)
                    excluded_set = ','.join(f':{col_name}' for col_name in df.columns)
                    if isinstance(conflict_key,list):
                        conflict_key = ','.join(str(e) for e in conflict_key)
                    match str(conflict_action).lower():
                        case 'update':
                            try:
                                placeholders = ','.join(f':{i+1}' for i in range(len(df.columns)))
                                INSERT_SQL = f"""
                                    MERGE INTO {schema}.{table_name} target
                                    USING (
                                        SELECT {placeholders} FROM dual
                                    ) source
                                    ON ({conflict_key})
                                    WHEN MATCHED THEN
                                        UPDATE SET {conflict_set}
                                    WHEN NOT MATCHED THEN
                                        INSERT ({col_names}) VALUES ({excluded_set})
                                """
                                cursor.executemany(INSERT_SQL, data)
                                self.execution_metrics['inserted_rows'] += len(data)
                                self.execution_metrics['updated_rows'] += cursor.rowcount
                            except Exception as e:
                                print('EXCEPTION ORACLE:',e)
                                raise e
                        case 'nothing':
                                INSERT_SQL = f"""
                                    INSERT INTO {schema}.{table_name} ({col_names})
                                        VALUES ({excluded_set})
                                    ON CONFLICT 
                                        ({conflict_key}) 
                                    DO NOTHING """
                                cursor.executemany(INSERT_SQL, data)
                                self.execution_metrics['inserted_rows'] += len(data)
                                self.execution_metrics['updated_rows'] += 0
                
            active_connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            print('EXCEPTION',e)
            raise e

