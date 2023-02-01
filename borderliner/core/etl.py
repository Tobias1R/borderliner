from .pipelines import (
    Pipeline, PipelineConfig
)

class EtlPipeline(Pipeline):
    def __init__(self, config: PipelineConfig | str, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
    
    def extract(self):
        self.logger.info('Extracting data')
        self.source.extract()
        

    def transform(self,*args,**kwargs):
        print('transform data in ETL class')
    
    def load_to_target(self,*args,**kwargs):
        self.logger.info('Loading data')
        self.target.load(self.source.data)

    
    def run(self, *args, **kwargs):
        self.extract()
        self.source._data = self.transform(self.source._data,*args, **kwargs)
        if self.config.dump_data_csv:
            for filename in self.source.csv_chunks_files:
                #file_name, bucket, object_name=None
                self.env.upload_file_to_storage(
                    file_name=filename,
                    storage_root=self.env.storage['storage_root'],
                    object_name=self.env.storage['temp_files_dir']+'/'+filename
                )
        self.load_to_target()
        #self.logger.info(self.target.metrics)
        