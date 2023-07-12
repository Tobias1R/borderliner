import math
from . import conn_abstract
from pandas._libs.lib import infer_dtype
from psycopg2.extras import execute_values
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import warnings
import pandas
from sqlalchemy.sql import text
from psycopg2 import Timestamp
from sqlalchemy.dialects.postgresql import insert
import psycopg2
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
        self.staging_schema = kwargs.get('staging_schema',None)
        self.staging_table = kwargs.get('staging_table',None)
        self.execution_metrics['staged_rows'] = 0
        

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
    
    def table_exists(self,table_name:str,schema:str):
        try:
            conn = self.engine.raw_connection()
            cur = conn.cursor()
            query = f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{schema}'"
            cur.execute(query)
            exists = cur.fetchone()[0]
            cur.close()
            conn.close()
            return exists
        except:
            return False
        #In this code, you first establish a connection to the PostgreSQL database using the psycopg2 library. Then, you execute a SQL query to check if the specified table




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
        if self.staging_schema != None:
            return self.insert_on_conflict_staged(
                active_connection,
                df,
                schema,
                table_name,
                if_exists,
                conflict_key,
                conflict_action
            )
        if isinstance(active_connection,Engine):
            connection = active_connection.raw_connection()
        else:
            connection = active_connection
        cursor = connection.cursor()
        cursor.execute('BEGIN;')
        try:
            
            self.execution_metrics['processed_rows'] += len(df)
            if self.use_count_for_metrics:
                total_rows_table_before = self.count_records(cursor,f'{schema}.{table_name}')
            else:
                total_rows_table_before = 0
            inserted_rows = 0
            updated_rows = 0
            if if_exists == 'append':
                for column in df.columns:
                    res = self.column_exists_db(
                        self.engine,
                        table_name, 
                        column, 
                        infer_dtype(df[column]))
                
                df.rename({'user':'"user"'},axis=1,inplace=True)
                col_names = ','.join(str(e) for e in df.columns)
                data = [tuple(x) for x in df.values]

                # Replace NaN's with None
                if self.kwargs.get('replace_nan',False):
                    data = [
                        tuple(
                            None if isinstance(x, float) and math.isnan(x) else x
                            for x in row
                        )
                        for row in data
                    ]
                
                if conflict_key != None:                    
                    conflict_set = ','.join(str(e) for e in df.columns)
                    excluded_set = ','.join('EXCLUDED.' + str(e) for e in df.columns)
                    if isinstance(conflict_key,list):
                        conflict_key = ','.join(str(e) for e in conflict_key)
                    match str(conflict_action).lower():
                        case 'update':
                            try:
                                INSERT_SQL = f"""
                                    INSERT INTO {schema}.{table_name} ({col_names})
                                        VALUES ({','.join(['%s'] * len(df.columns))})
                                        ON CONFLICT ({conflict_key}) DO UPDATE SET ({conflict_set})=({excluded_set})
                                        ;
                                """
                                
                                cursor.executemany(INSERT_SQL, data)
                                inserted_rows += cursor.rowcount
                                
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
            
            cursor.execute('COMMIT;')
            #connection.commit()   
            if self.use_count_for_metrics:    
                total_rows_table_after = self.count_records(cursor,f'{schema}.{table_name}') 
                inserted_rows_temp =  total_rows_table_after - total_rows_table_before
                self.execution_metrics['inserted_rows'] += inserted_rows_temp
                if inserted_rows_temp == 0:
                    updated_rows = inserted_rows
                self.execution_metrics['updated_rows'] += updated_rows   
            else:
                self.execution_metrics['inserted_rows'] += inserted_rows
                 

        except Exception as e:  
            cursor.execute('ROLLBACK;') 
            cursor.close()
            connection.close()         
            raise Exception('db exception:'+str(e))
        cursor.close()
        #connection.close()
    
    def insert_on_conflict_staged(
        self, 
        active_connection,
        df, 
        schema,
        table_name,
        if_exists='append',
        conflict_key=None,
        conflict_action=None):

        if isinstance(active_connection,Engine):
            connection = active_connection.raw_connection()
        else:
            connection = active_connection
        cursor = connection.cursor()
        cursor.execute('BEGIN;')
        try:
            
            self.execution_metrics['processed_rows'] += len(df)
            if self.use_count_for_metrics:
                total_rows_table_before = self.count_records(cursor,f'{schema}.{table_name}')
            else:
                total_rows_table_before = 0
            inserted_rows = 0
            updated_rows = 0
            truncate_staging_statement = f'TRUNCATE TABLE {self.staging_schema}.{self.staging_table};'
            self.logger.info(f'Truncating {self.staging_schema}.{self.staging_table}')
            cursor.execute(truncate_staging_statement)
            join_key = ''
            update_where_clause = ''
            update_where_clause2 = ''
            bulk_insert_where_clause = ''
            if isinstance(conflict_key,list):
                index = 0
                for key in conflict_key:
                    join_key += 'ods1.' +str(key)+' = '+'stg.'+key 
                    update_where_clause += 'ods1.' +str(key)+' = '+'stg.'+key
                    update_where_clause2 += 'ods.' +str(key)+' = '+'stg.'+key
                    bulk_insert_where_clause += 'ods1.' +str(key)+' IS NULL'
                    if index+1 < len(conflict_key):
                        join_key += ' AND \n' 
                        update_where_clause += ' AND \n'
                        update_where_clause2 += ' AND \n'
                        bulk_insert_where_clause += ' AND \n'
                    index += 1

                conflict_key = ','.join(str(e) for e in conflict_key)
            else:
                join_key = 'ods1.'+conflict_key+' = '+'stg.'+conflict_key
                update_where_clause += 'ods1.' +conflict_key+' = '+'stg.'+conflict_key
                update_where_clause2 += 'ods.' +conflict_key+' = '+'stg.'+conflict_key
                bulk_insert_where_clause += 'ods1.' +conflict_key+' IS NULL'

            data = [tuple(x) for x in df.values]
            col_names = ','.join(str(e) for e in df.columns)
            update_set = ''
            index = 0
            for col in df.columns:
                update_set += str(col) + ' = ' + 'stg.' + col 
                if index+1 < len(df.columns):
                        update_set += ',\n' 
                index += 1
            
            # save data in staging table
            INSERT_SQL = f"""INSERT INTO 
                {self.staging_schema}.{self.staging_table} ({col_names})
                VALUES %s """
            total_bulk = len(data)
            self.logger.info(f'Bulk insert {total_bulk} records')

            execute_values(
                    cursor, 
                    INSERT_SQL, 
                    data, 
                    template=None, 
                    page_size=10000)
            self.execution_metrics['staged_rows'] += cursor.rowcount
            # update
            # TODO REMOVE THIS URGENTLY
            update_where_clause = 'ods1.brdr_data_md5 is not null and stg.brdr_data_md5 != ods1.brdr_data_md5'
            if str(conflict_action).upper() == 'UPDATE':
                UPDATE_SQL = f"""
                    UPDATE {schema}.{table_name} ods SET {update_set}
                    FROM (SELECT stg.* FROM  {self.staging_schema}.{self.staging_table} stg  
                        LEFT JOIN {schema}.{table_name} ods1 
                        ON {join_key} 
                        WHERE {update_where_clause}) as stg
                    WHERE {update_where_clause2}
                """
                
                self.logger.info('Performing updates.')
                cursor.execute(UPDATE_SQL)
                updated_rows += cursor.rowcount
                #self.execution_metrics['updated_rows'] += cursor.rowcount

            BULK_INSERT_SQL = f"""
                INSERT INTO {schema}.{table_name}
                (SELECT stg.* FROM {self.staging_schema}.{self.staging_table} stg
                    LEFT JOIN {schema}.{table_name} ods1 
                    ON {join_key} WHERE {bulk_insert_where_clause})
            """

            #print(BULK_INSERT_SQL)
            self.logger.info('Moving data from staging to ods.')
            cursor.execute(BULK_INSERT_SQL)
            inserted_rows += cursor.rowcount
            #self.execution_metrics['inserted_rows'] += cursor.rowcount
            cursor.execute('COMMIT;')
            #connection.commit()  
            if self.use_count_for_metrics:
                total_rows_table_after = self.count_records(cursor,f'{schema}.{table_name}') 
                inserted_rows_temp =  total_rows_table_after - total_rows_table_before
                self.execution_metrics['inserted_rows'] += inserted_rows_temp
                if inserted_rows_temp == 0:
                    updated_rows = inserted_rows
                self.execution_metrics['updated_rows'] += updated_rows 
            else:
                self.execution_metrics['inserted_rows'] += inserted_rows
                self.execution_metrics['updated_rows'] += updated_rows
                                          
        except Exception as e:  
            cursor.execute('ROLLBACK;') 
            cursor.close()
            connection.close()         
            raise Exception('db exception:'+str(e))
        cursor.close()
        
Connection = PostgresBackend