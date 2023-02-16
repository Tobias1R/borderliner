import time
from .pipelines import (
    Pipeline, PipelineConfig
)
import pandas
import yaml
import hashlib

def gen_md5(df:pandas.DataFrame,ignore=[])->pandas.Series:
    """Generate data md5"""
    df = df.assign(concat=str(''))    
    for col in df:
        if col not in ignore:
            df['concat'] += df[col].astype(str)
    df['md5'] = df['concat'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    df = df.drop('concat', axis=1)
    return df['md5']


class EtlPipeline(Pipeline):
    def __init__(self, config: PipelineConfig | str, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
    
    def extract(self):
        
        self.tracker.phase(f'Extracting data from {self.source}')
        self.source.extract()
        if self.config.generate_control_columns:
            self.logger.info('setting up control columns')
            
            if isinstance(self.source._data,pandas.DataFrame):
                self.source._data['data_md5'] = gen_md5(
                        self.source._data,
                        ignore=self.config.ignore_md5_fields
                    )
                self.source._data['extract_date'] = str(time.strftime("%Y%m%d%H%M%S"))
                
            elif isinstance(self.source._data,list):
                newlist = []
                for df in self.source._data:
                    df['data_md5'] = gen_md5(
                        df,
                        ignore=self.config.ignore_md5_fields
                    )
                    df['extract_date'] = str(time.strftime("%Y%m%d%H%M%S"))
                    newlist.append(df)
                self.source._data = newlist
        else:
            self.logger.info('skipping control columns')
        

    def transform(self,*args,**kwargs):
        self.logger.warning('A middleware function is required to transform data in ETL class')
    
    def load_to_target(self,*args,**kwargs):
        if self.kwargs.get('no_target',False):
            return
        self.tracker.phase(f'Loading data to {self.target}')
        self.target.load(self.source.data)

    
    def run(self, *args, **kwargs):
        self.extract()
        self.source._data = self.transform(self.source._data,*args, **kwargs)
        if self.config.dump_data_csv:
            for filename in self.source.csv_chunks_files:
                #file_name, bucket, object_name=None
                self.env.upload_file_to_storage(
                    file_name=filename,
                    storage_root=self.env.storage_paths['storage_root'],
                    object_name=self.env.storage_paths['temp_files_dir']+'/'+filename
                )
        self.load_to_target()
        #self.logger.info(self.target.metrics)
        