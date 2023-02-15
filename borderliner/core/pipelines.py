from datetime import datetime
import logging
import os
import sys
import time
from typing import Union, TextIO
import yaml
from .exceptions import PipelineConfigException
from .sources import (
    PipelineSource,
    PipelineSourceDatabase,
    PipelineSourceApi,
    PipelineSourceFlatFile
)
from .targets import (
    PipelineTarget,
    PipelineTargetApi,
    PipelineTargetDatabase,
    PipelineTargetFlatFile
    )
from borderliner.cloud import CloudEnvironment
from borderliner.cloud.Aws import AwsEnvironment

PIPELINE_TYPE_PROCESS = 'PROCESS_PIPELINE'
PIPELINE_TYPE_EXTRACT = 'EXTRACT_PIPELINE'
PIPELINE_TYPE_ETL = 'ETL_PIPELINE'


# logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()

class PipelineConfig:
    
    def __init__(self,
                source:Union[str, TextIO],
                ) -> None:
        self.pipeline_method = 'INCREMENTAL'
        self.perform_updates = False
        self.transform_data = False
        self.named_queries = {}
        self.named_queries_params = {}
        self.queries = []
        self.extract_query = ''
        self.insert_query = ''
        self.update_query = ''
        self.extract_query_params = {}
        self.insert_query_params = {}
        self.update_query_params = {}
        self.pipeline_name = ''
        self.pipeline_type = ''
        self.source = {}
        self.target = {}
        self.csv_filename_prefix = ''
        self.dump_data_csv = False
        

        self.md5_ignore_fields = []
        # cloud env
        self.storage = {}
        # clear dump files after action
        self.clear_dumps = False

        try:
            f = open(source,'r+')
            logger.info(f'loading file {source}')
            self._load_from_file(f)
        except:
            try:
                self._load_from_file(source)
            except:
                raise ValueError('Impossible to open config file.')
        
        
            

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __str__(self):
        return str(self.__dict__)

    def _load_config_from_redshift(self):
        pass

    def _load_from_file(self,file):
        data_loaded = yaml.safe_load(file)
        for key in data_loaded:
            # search for $env vars
            if isinstance(data_loaded[key],dict):
                for k in data_loaded[key]:                    
                    if str(data_loaded[key][k]).startswith('$ENV_'):
                        env_key = str(data_loaded[key][k]).replace('$ENV_','')#str(key) + '_' + str(k)
                        data_loaded[key][k] = os.getenv(
                                env_key,
                                data_loaded[key][k]
                            )   
                    elif str(data_loaded[key][k]).startswith('$airflow'):
                        env_key = 'AIRFLOW_VAR_'+str(key).upper() + '_' + str(k).upper()
                        data_loaded[key][k] = os.getenv(
                                env_key,
                                data_loaded[key][k]
                            ) 
                        print('LOADED ',env_key,data_loaded[key][k])                 
            self.__setattr__(key,data_loaded[key])




class Pipeline:
    def __init__(self,config:PipelineConfig|str,*args,**kwargs) -> None:
        """
        The init method initializes the attributes of a Pipeline object.

        The method first creates a runtime attribute to store the current 
            datetime and a pid attribute to store a unique string identifier 
            for the pipeline.
        The logger attribute is initialized to store the logger object.
        The env attribute is initialized as None, which is an instance of 
            the CloudEnvironment class.
        The name and pipeline_type attributes are both initialized with 
            default strings.
        The config attribute is initialized as None, which is an instance 
            of the PipelineConfig class.
        The method then checks if the argument passed as config is a string 
            or an instance of the PipelineConfig class and sets the value of 
                config accordingly.
        The source and target attributes are both initialized as None and will 
            store instances of the PipelineSource and PipelineTarget classes, 
                respectively.
        The method then calls the _configure_pipeline method and passes kwargs 
            to it.

        kwargs
        no_source: True for manual specification of source
        no_target: True for manual specification of target
        """
        self.runtime = datetime.now()
        self.pid = str(time.strftime("%Y%m%d%H%M%S")) + str(os.getpid())
        self.logger = logging.getLogger()
        self.logger.info('Initializing...')
        self.env:CloudEnvironment = None

        self.name:str = 'PIPELINE_NAME'
        self.pipeline_type:str = PIPELINE_TYPE_PROCESS
        self.config:PipelineConfig = None
        
        if isinstance(config,str):
            if os.path.isfile(config):
                self.config = PipelineConfig(config)
        elif isinstance(self.config,PipelineConfig):
            self.config = config
        else:
            raise PipelineConfigException("""
                Impossible to configure pipeline
            """)
        
        self.source:PipelineSource = None
        self.target:PipelineTarget = None
        
        self._configure_pipeline(kwargs)

        self.logger.info(f'{str(self.__class__)} loaded.')

    def _configure_pipeline(self,*args,**kwargs):
        self._configure_environment(self.config['cloud'])
        
        if not kwargs.get('no_source',None):
            self.make_source()

        if not kwargs.get('no_target',None):
            self.make_target()
        
        

    def _configure_environment(self,config:dict):
        service = config.get('service',None)
        match str(service).upper():
            case 'AWS':
                self.logger.info(f'loading {service} environment')
                self.env = AwsEnvironment(config)
    
    def make_source(self,src=None):
        """
            Makes a data source based on the given `src` argument or 
            the default source defined in `config.source`.

            The function supports three types of data sources:
            - 'DATABASE': A database source is created using the `PipelineSourceDatabase` class.
            - 'FILE': A flat file source is created using the `PipelineSourceFlatFile` class.
            - 'API': An API source is created using the `PipelineSourceApi` class.

            Parameters:
            src (dict, optional): A dictionary containing the source 
                configuration. If `None`, the default source
                defined in `config.source` will be used.

            Raises:
            ValueError: If the data source type specified in `src` 
                is not supported.

        """
        if src == None:
            src = self.config.source

        if isinstance(src,dict):
            match str(src['source_type']).upper():
                case 'DATABASE':
                    self.source = PipelineSourceDatabase(
                        src,
                        dump_data_csv=self.config.dump_data_csv,
                        pipeline_pid=self.pid
                    )
                    return
                case 'FILE':
                    self.source = PipelineSourceFlatFile(src,
                        enviroment=self.env,
                        pipeline_pid=self.pid)
                    return
                case 'API':
                    self.source = PipelineSourceApi(src)
                    return
        raise ValueError('Unknown data source')

    def make_target(self,tgt=None):
        """
            This method creates an instance of the data source for the pipeline.

            The source of the data can be a database, flat file, or API. The source 
            type is determined by the src parameter. If src is not provided, the
            source is taken from the pipeline configuration.

            If the source type is a dictionary, the method uses a match statement 
            to determine the correct source type, and creates an instance of the 
            appropriate class, either PipelineSourceDatabase, PipelineSourceFlatFile,
            or PipelineSourceApi.

            If the source type is unknown, a ValueError is raised.
        """
        if tgt == None:
            tgt = self.config.target

        if isinstance(tgt,dict):
            match str(tgt['target_type']).upper():
                case 'DATABASE':
                    self.target = PipelineTargetDatabase(
                        tgt,
                        dump_data_csv=self.config.dump_data_csv,
                        pipeline_pid=self.pid,
                        csv_chunks_files=self.source.csv_chunks_files)
                    return
                case 'FILE':
                    self.target = PipelineTargetFlatFile(tgt)
                    return
                case 'API':
                    self.target = PipelineTargetApi(tgt)
                    return
        raise ValueError('Unknown data target')

    def find_entry_point(self,*args,**kwargs):
        self.run()
        self.after_run()

    def run(self,*args,**kwargs):
        pass

    def after_run(self,*args,**kwargs):
        self.print_metrics()

    def print_metrics(self):
        for metric, value in self.target.metrics.items():
            self.logger.info(f"{metric.capitalize()}: {value}")


    def get_query(self,query:str='extract'):
        if query == 'extract':
            return self.config.extract_query.format(
                **self.config.extract_query_params)
        if query == 'bulk_insert':
            return self.config.insert_query.format(
                **self.config.insert_query_params
            )
        if query == 'update':
            return self.config.update_query.format(
                **self.config.update_query_params
            )
        if query in self.config.named_queries:
            return str(self.config.named_queries[query]).format(
                **self.config.named_queries_params[query]
            )
        raise Exception('Query not found.')
    
    def transform(self,*args,**kwargs):
        print('''
            Function transform in ETL class has no effect on data.
            If you wan't to transform your data you need to assign 
            a middleware function to perform this operation!
            ''')
