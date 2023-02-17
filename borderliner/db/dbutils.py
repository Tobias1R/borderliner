
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