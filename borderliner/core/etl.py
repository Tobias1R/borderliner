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
            data_md5_label = self.get_control_columns_names().get('data_md5_label','brdr_data_md5')
            extract_date_label = self.get_control_columns_names().get('extract_date_label','brdr_extract_date')
            if isinstance(self.source._data,pandas.DataFrame):
                self.source._data[data_md5_label] = gen_md5(
                        self.source._data,
                        ignore=self.config.ignore_md5_fields
                    )
                self.source._data[extract_date_label] = str(time.strftime("%Y%m%d%H%M%S"))
                
            elif isinstance(self.source._data,list) or isinstance(self.source._data,type(iter([]))):
                newlist = []
                
                for df in self.source._data:
                    df[data_md5_label] = gen_md5(
                        df,
                        ignore=self.config.ignore_md5_fields
                    )
                    df[extract_date_label] = str(time.strftime("%Y%m%d%H%M%S"))
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
        print_upload_info = True
        #self.source._data = self.transform(self.source._data,*args, **kwargs)
        if self.config.dump_data_csv:
            for filename in self.source.csv_chunks_files:
                #file_name, bucket, object_name=None
                df = pandas.read_parquet(filename)
                # collect meta info
                meta_info = self.extract_meta_info(df)
                self.data_lineage['source_data'] = meta_info

                transformed_data = self.transform(df,*args, **kwargs)
                if isinstance(transformed_data, pandas.DataFrame):
                    df = transformed_data
                    # collect meta info
                    meta_info = self.extract_meta_info(df)
                    self.data_lineage['transformed_data'] = meta_info
                df.to_parquet(filename)
                if self.config.upload_dumps_to_storage:
                    self.env.upload_file_to_storage(
                        file_name=filename,
                        storage_root=self.env.storage_paths['storage_root'],
                        object_name=self.env.storage_paths['temp_files_dir']+'/'+filename
                    )
                else:
                    if print_upload_info:
                        self.logger.info("File uploads disabled in pipeline configuration.")
                        print_upload_info = False
        else:
            self.source._data = self.transform(self.source._data,*args, **kwargs)
            # if isinstance(self.source._data,pandas.DataFrame):
            #     # collect meta info
            #     meta_info = self.extract_meta_info(self.source._data)
            #     self.data_lineage['source_data'] = meta_info
            #     # transform
                
            #     meta_info = self.extract_meta_info(self.source._data)
            #     self.data_lineage['transformed_data'] = meta_info
            # elif isinstance(self.source._data,list) or isinstance(self.source._data,type(iter([]))):
            #     newlist = []
            #     for df in self.source._data:
            #         # collect meta info
            #         meta_info = self.extract_meta_info(df)
            #         self.data_lineage['source_data'] = meta_info
            #         # transform
            #         df = self.transform(self.source._data,*args, **kwargs)
            #         meta_info = self.extract_meta_info(df)
            #         self.data_lineage['transformed_data'] = meta_info
            #         newlist.append(df)
            #     self.source._data = newlist

            
        self.load_to_target()
        # target flat file only
        if self.config.target.get('save_copy_in_storage',False):
            filename = self.target.get_filename()
            self.env.upload_file_to_storage(
                        file_name=filename,
                        storage_root=self.env.storage_paths['storage_root'],
                        object_name=self.env.storage_paths['temp_files_dir']+'/'+filename
                    )
        #self.logger.info(self.target.metrics)
        