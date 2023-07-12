import os
from borderliner.admin.commands import ParseTemplateCommand
import yaml

class CommandCreateEtl(ParseTemplateCommand):
    def __init__(self, name, template_path,*args,**kwargs):
        super().__init__(name, template_path,*args,**kwargs)
        #self.parser.add_argument('--source_type', type=str, help='The source to use', default='DATABASE')
        #self.parser.add_argument('--target_type', type=str, default='DATABASE', help='The target to use')
        source_db = {
            "source":{
                #"source_type": "DATABASE",
                #"type": f"$ENV_source",
                "host": f"$ENV_source_host",
                "port": f"$ENV_source_port",
                "database": f"$ENV_source_database",
                "schema": f"$ENV_source_schema",
                "username": f"$ENV_source_username",
                "password": f"$ENV_source_password",
                "connection_try": f"$ENV_source_connection_try",
                "sleep_try": f"$ENV_source_sleep_try",
                "table": f"$ENV_source_table",
                "connection": "new",
                "queries": {
                    "extract": f"SELECT DISTINCT * from $ENV_source_schema.$ENV_source_table WHERE marketday = '{{marketday}}' order by marketday,period limit 10000;",
                    "iterate": f"select distinct marketday from $ENV_source_schema.$ENV_.source_table limit 10;"
                }
            }
        }
        target_db = {
            "target": {
                #"target_type": "DATABASE",
                #"type": f"$ENV_target",
                "host": f"$ENV_target_host",
                "port": f"$ENV_target_port",
                "database": f"$ENV_target_database",
                "schema": f"$ENV_target_schema",
                "username": f"$ENV_target_username",
                "password": f"$ENV_target_password",
                "connection_try": f"$ENV_target_connection_try",
                "sleep_try": f"$ENV_target_sleep_try",
                "table": f"$ENV_target_table",
                "connection": "new",
                "conflict_key": "brdr_data_md5",
                "conflict_action": "update"
            }
        }

        source_file = {
            "source":{
                #"source_type": "FILE",
                #"type": "flat_file",
                "extension": "csv",
                "read_csv_params": {
                    "header": 0
                },
                "storage": {
                    "directory": "bucket_name",
                    "prefix": "files/queue",
                    "list_options": {
                        "order": "date desc",
                        "number_of_files": 1,
                        "regex_pattern": False
                    }
                },
                "archive": {
                    "directory": "bucket",
                    "prefix": "files/archive"
                },
                "move_files_to_archive": False,
                "remove_files": False
            }
        }
                
        match self.get_argument('source').upper():
            case 'DATABASE':
                self.source = source_db
            case 'FILE':
                self.source = source_file
            case _ :
                self.source = source_db
        
        self.target = target_db
        
        self.etl_config = {
            "type": "ETL",
            "cloud": "cloud.yml",
            "pipeline_name": f"$ENV_pipeline_name",
            "dump_data_csv": False,
            "generate_control_columns": True,
            "create_target_tables": True,
            "use_data_md5_as_primary_key":True,
            "alchemy_log_level": "INFO",
            
        }
        
        merged_dict = self.etl_config.copy()
        merged_dict.update(self.source)
        merged_dict.update(self.target)
        self.merged_dict = merged_dict
        # for key, value in merged_dict.items():
        #     if isinstance(value,dict):
        #         for subkey, subvalue in value.items():
        #             if isinstance(subvalue, dict):
        #                 for subsubkey, subsubvalue in subvalue.items():
        #                     default_val = os.getenv(f"{key}_{subkey}_{subsubkey}", f"$ENV_{subsubkey}")
        #                     self.parser.add_argument(f'--{key}_{subkey}_{subsubkey}', type=str, help=f'{subsubkey} value for {subkey} {key}', default=default_val)
        #             else:
        #                 default_val = os.getenv(f"{key}_{subkey}", f"$ENV_{subkey}")
        #                 self.parser.add_argument(f'--{key}_{subkey}', type=str, help=f'{subkey} value for {key}', default=default_val)
        #     elif isinstance(value,str):
        #         subkey = value
        #         default_val = os.getenv(f"{key}_{subkey}", f"$ENV_{subkey}")
        #         self.parser.add_argument(f'--{key}_{subkey}', type=str, help=f'{subkey} value for {key}', default=default_val)
        
        print(self.generate_config())

    def generate_config(self):
        print(self.extra_args)
        self.args.__setattr__('config_yml_file',self.get_argument('pipeline_name')+"_config.yml")
        self.merged_dict['source']['source_type'] = self.get_argument('source_type')
        self.merged_dict['target']['target_type'] = self.get_argument('target_type')
        # Dump the dictionary to YAML
        return yaml.dump(
                self.merged_dict, 
                default_flow_style=False)
    


        