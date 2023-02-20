# BORDERLINER
### The insightful data pipeline framework

## Install
requires python 3.11.1

```bash
    python setup.py install
```

## ETL Example
```python
from borderliner.core import pipelines,etl
import pandas

def transform(data:pandas.DataFrame,*args,**kwargs):
    
    def parse_df(df):
        df['timestamp_utc'] = df['timestamp_utc'].astype(str)
        df['data'] = df['data'].astype(str)
        df['dia'] = df['dia'].astype(str)
        df['nome_ficheiro'] = 'that was another test'
        return df
    
    if isinstance(data,list):
        for df in data:
            # timestamp fields to str
            df = parse_df(df)
        return data
    if isinstance(data,pandas.DataFrame):
        df = parse_df(data)
        return df

if __name__ == '__main__':
    p = etl.EtlPipeline("etl.yml")
    p.transform = transform
    p.find_entry_point()
```

this example uses this config file

```yaml
type: 'ETL'

cloud:
  service: AWS
  task_manager: AIRFLOW
  storage:
    temp_files_dir: temp_files_v2
    storage_root: $ENV_main_bucket

# set true i'll force pipeline to pass data through csv dumps
# and don't hold the entire dataset on memory.
# some database supports COPY method from cloud storage
# you can define the insertion_method: copy on target
dump_data_csv: True

source:
  source_type: DATABASE
  type: 'postgres'
  host: "host.docker.internal"
  port: 5433
  database: postgres
  schema: public
  username: postgres
  password: $ENV_devpg_pwd
  connection_try: 4
  sleep_try: 2
  table: your_table
  connection: new
  queries:
    extract: SELECT * from public.your_table; 
    iterate: select distinct versao from public.your_table limit 10;

# REDSHIFT TARGET WITH UPDATE BY KEY
target:
  target_type: DATABASE
  type: 'redshift'
  host: $ENV_ods_host
  port: 5439
  database: $ENV_ods_db
  staging_schema: staging
  staging_table: your_table
  schema: public
  username: $ENV_ods_rw_user
  password: $ENV_ods_rw_pwd
  connection_try: 4
  sleep_try: 2
  table: your_table
  connection: new
  conflict_key: ['timestamp_utc', 'dia_mercado', 'hora_mercado', 'versao']
  conflict_action: update
```