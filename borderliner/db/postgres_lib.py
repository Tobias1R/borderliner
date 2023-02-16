from . import conn_abstract
from pandas._libs.lib import infer_dtype
from psycopg2.extras import execute_values
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import warnings
import pandas
from sqlalchemy.sql import text
from psycopg2 import Timestamp

class PostgresBackend(conn_abstract.DatabaseBackend):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.interface_name = 'postgres'
        self.alchemy_engine_flag = 'postgresql'
        self.expected_connection_args = [
                'host',
                'database',
                'user',
                'password',
                'port'
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
            col_type = "datetime"

        elif col_type == "empty":
            col_type = "string"

        elif col_type == "complex":
            raise ValueError('Complex datatypes not supported')

        if col_type not in _SQL_TYPES:
            col_type = "string"

        return _SQL_TYPES[col_type]

    #@staticmethod
    def column_exists_db(
        self,
        active_connection:Engine,
        table_name, 
        column_name, 
        dtype, 
        if_not_exists='append'):
        """
        Check if colunm exists on db.

        @TODO: This only works with postgres databases. Need a method 
        for all attended databases types.
        """
        q = f"SELECT count(*) FROM information_schema.columns " \
            f"where table_name = '{table_name}' and column_name = '{column_name}'"
        conn = active_connection
        try:          
            if conn.execute(q).fetchone()[0] == 1:
                return 0
            elif conn.execute(q).fetchone()[0] == 0 and if_not_exists == 'append':
                dt = PostgresBackend._sql_type_name(dtype.__str__())
                qc = f"ALTER TABLE {table_name} ADD COLUMN {column_name.lower()} {dt}"
                conn.execute(qc)
                #active_connection.commit()
                #self.logger.log(self.logger.CRITICAL, ("%s column didn't existed in the %s. Added." % (column_name, table_name)))
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
                    conflict_set = ','.join(str(e) for e in df.columns)
                    excluded_set = ','.join('EXCLUDED.' + str(e) for e in df.columns)
                    if isinstance(conflict_key,list):
                        conflict_key = ','.join(str(e) for e in conflict_key)
                    match str(conflict_action).lower():
                        case 'update':
                            try:
                                for d in data:
                                    INSERT_SQL = f"""
                                        WITH t as (
                                        INSERT INTO {schema}.{table_name} ({col_names})
                                            VALUES {d}
                                        ON CONFLICT 
                                            ({conflict_key}) 
                                        DO UPDATE SET
                                            ({conflict_set})=({excluded_set}) RETURNING xmax)
                                            SELECT COUNT(*) AS all_rows, 
                                            SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS ins, 
                                            SUM(CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END) AS upd 
                                        FROM t;"""
                                    
                                    cursor.execute(INSERT_SQL)
                                    metrics = cursor.fetchall()
                                    inserts = metrics[0][1]
                                    updates = metrics[0][2]
                                    self.execution_metrics['inserted_rows'] += int(inserts)
                                    self.execution_metrics['updated_rows'] += int(updates)
                                    
                            except Exception as e:
                                #print(values,INSERT_SQL)
                                print('EXCEPTION PGLIBS:',e)
                                raise e
                        case 'nothing':
                                INSERT_SQL = f"""
                                    INSERT INTO {schema}.{table_name} ({col_names})
                                        VALUES %s
                                    ON CONFLICT 
                                        ({conflict_key}) 
                                    DO NOTHING """
                                execute_values(
                                        cursor, 
                                        INSERT_SQL, 
                                        data, 
                                        template=None, 
                                        page_size=10000)
                                self.execution_metrics['inserted_rows'] += cursor.rowcount
                else:
                    INSERT_SQL = f"""
                        INSERT INTO {schema}.{table_name} ({col_names})
                            VALUES %s
                        """
                    execute_values(cursor, 
                                    INSERT_SQL, 
                                    data, 
                                    template=None, 
                                    page_size=10000)
                    self.execution_metrics['inserted_rows'] += cursor.rowcount
                
                connection.commit()                                
                cursor.close()
                connection.close()
                
        except Exception as e:            
            raise Exception('db exception:'+str(e))
Connection = PostgresBackend