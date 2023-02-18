from sqlalchemy import types
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
        'VARCHAR': types.String,
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
        'DOUBLE_PRECISION': types.Numeric
    }

    try:
        return type_map[db_type.upper()]
    except KeyError:
        raise ValueError(f'Unsupported data type: {db_type}')