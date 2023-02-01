from . import conn_abstract

class IbmDB2Connection(conn_abstract.PipelineConnection):
    def __init__(self,*args,**kwargs):
        self.interface_name = 'DB2'
        self.alchemy_engine_flag = 'iaccess+pyodbc'
        self.expected_connection_args = [
                'host',
                'user',
                'password'
            ]
        self.database = 'dummy?DBQ=DEFAULT_SCHEMA'
    
    def get_connection(self, *args, **kwargs):
        return self.get_engine(*args, **kwargs)

Connection = IbmDB2Connection