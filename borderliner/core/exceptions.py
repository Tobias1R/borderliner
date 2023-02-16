class PipelineConfigException(Exception):
    pass

class BorderlinerException(Exception):
    pass

class InvalidCloudConfigurationException(BorderlinerException):
    msg = "Invalid cloud configuration exception."
    pass