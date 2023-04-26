import re
from sqlalchemy import (
    create_engine, 
    MetaData, 
    Table, 
    Column, 
    Integer, 
    String, 
    UniqueConstraint,
    types
)

def add_unique_constraint(table_name, constraint_fields, engine):
    # create the metadata and reflect the existing table
    metadata = MetaData(bind=engine)
    metadata.reflect()
    table = metadata.tables[table_name]
    
    # create the unique constraint expression
    fields = [getattr(table.c, field) for field in constraint_fields]
    unique_constraint = UniqueConstraint(*fields, name=f'uq_{table_name}_{"_".join(constraint_fields)}')
    
    # generate the SQL statement and execute it
    sql_statement = str(unique_constraint.compile(dialect=engine.dialect))
    with engine.connect() as conn:
        conn.execute(sql_statement)

db2_to_postgres = {
    'SMALLINT': 'SMALLINT',
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'DECIMAL': 'NUMERIC',
    'NUMERIC': 'NUMERIC',
    'REAL': 'REAL',
    'DOUBLE': 'DOUBLE PRECISION',
    'FLOAT': 'DOUBLE PRECISION',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'TIMESTAMP': 'TIMESTAMP',
    'VARCHAR': 'VARCHAR',
    'CHAR': 'CHAR',
    'CLOB': 'TEXT',
    'BLOB': 'BYTEA',
}

# Define a function to generate a DDL statement for a single column
def column_ddl(col_info):
    name, db2_type, length, _ = col_info
    postgres_type = db2_to_postgres[db2_type.strip()]
    if length:
        return f"{name} {postgres_type}({length})"
    else:
        return f"{name} {postgres_type}"

def get_column_type(db_type: str) -> types.TypeEngine:
    type_map = {
        'NVARCHAR': types.String,
        'TEXT': types.Text,
        'INTEGER': types.Integer,
        'BIGINT': types.BigInteger,
        'FLOAT': types.Float,
        'DOUBLE': types.Float,
        'DECIMAL': types.Numeric,
        'NUMERIC': types.Numeric,
        'BOOLEAN': types.Boolean,
        'DATE': types.Date,
        'DATETIME': types.DateTime,
        'TIMESTAMP': types.DateTime,
        'TIME': types.Time,
        'VARCHAR2': types.String,
        'CHAR': types.String,
        'NCHAR': types.String,
        'NUMBER': types.Numeric,
        'SMALLINT': types.SmallInteger,
        'REAL': types.Float,
        'BLOB': types.LargeBinary,
        'CLOB': types.Text,
        'DATETIME2': types.DateTime,
        'DATETIMEOFFSET': types.DateTime,
        'TIME': types.Time,
        'TIMESTAMP': types.DateTime,
        'BIT': types.Boolean,
        'TINYINT': types.SmallInteger,
        'MEDIUMINT': types.Integer,
        'JSON': types.JSON,
        'MONEY': types.Numeric,
        'UUID': types.String,
        'XML': types.Text,
        'DATETIMEOFFSET': types.DateTime,
        'DATETIME2': types.DateTime,
        'STRING': types.String,
        'DOUBLE_PRECISION': types.Float
    }

    try:
        new_string = re.sub(r'.*CHAR\(\d+\)', 'CHAR', db_type.upper())
        new_string = re.sub(r'\(\d+, \d+\)', '', new_string)
        new_string = re.sub(r'\(\d+,\d+\)', '', new_string)
        return type_map[new_string]
    except KeyError:
        raise ValueError(f'Unsupported data type: {db_type}')