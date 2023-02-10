from .pipelines import (
    Pipeline, PipelineConfig
)

class ProcessPipeline(Pipeline):
    def __init__(self, config: PipelineConfig | str, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)
        
    
    