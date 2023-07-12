import os
import requests

class AirflowAPI:
    def __init__(self, base_url, username, password):
        if str(base_url).strip() == "":
            base_url = os.environ.get("AIRFLOW_BASE_URL")
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password

    def _make_request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.request(method, url, auth=(self.username, self.password), **kwargs)
        response.raise_for_status()
        return response.json()

    def get_dags(self):
        return self._make_request("GET", "/api/v1/dags")

    def get_dag(self, dag_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}")

    def trigger_dag(self, dag_id, run_id=None, conf=None):
        data = {}
        if run_id:
            data["run_id"] = run_id
        if conf:
            data["conf"] = conf
        return self._make_request("POST", f"/api/v1/dags/{dag_id}/dagRuns", json=data)

    def get_dag_runs(self, dag_id,**kwargs):
        url = f"/api/v1/dags/{dag_id}/dagRuns"
        if kwargs:
            url += '?'
            is_first = True
            for key,value in kwargs.items():
                if is_first:
                    url += str(key) +'='+ str(value)
                    is_first = False
                else:
                    url += '&'+str(key) +'='+ str(value)
        else:
            url = f"/api/v1/dags/{dag_id}/dagRuns?limit=365"
        print("get_dag_runs:",url)
        return self._make_request("GET", url,)

    def get_dag_run(self, dag_id, run_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{run_id}")
    
    def get_dag_details(self, dag_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/details")
    
    def get_health(self,**kwargs):
        return self._make_request("GET", f"/api/v1/health")
    
    def get_variables(self):
        return self._make_request("GET", f"/api/v1/variables")
    
    def get_variable(self, key):
        return self._make_request("GET", f"/api/v1/variables/{key}")
    
    def get_pools(self):
        return self._make_request("GET", f"/api/v1/pools")
    
    def get_pool(self, pool_name):
        return self._make_request("GET", f"/api/v1/pools/{pool_name}")
    
    def get_connections(self):
        return self._make_request("GET", f"/api/v1/connections")
    
    def get_connection(self, connection_id):
        return self._make_request("GET", f"/api/v1/connections/{connection_id}")
    
    def get_xcom(self, dag_id, task_id, dag_run_id, key):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}")
    
    def get_task_instance(self, dag_id, task_id, dag_run_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}")
    
    def get_task_instances(self, dag_id, dag_run_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
    
    def get_task_instance_log(self, dag_id, task_id, dag_run_id, execution_date):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{execution_date}")
    
    def get_task_instance_logs(self, dag_id, task_id, dag_run_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs")
    
    def get_task_instance_xcoms(self, dag_id, task_id, dag_run_id):
        return self._make_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries")
    
    def set_variable(self, key, value):
        return self._make_request("PATCH", f"/api/v1/variables/{key}", json={"key":key, "value": value})
    
    def set_note(self, dag_id, dag_run_id, note):
        ''' Set a note for a dag run '''
        return self._make_request("PATCH", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/setNote", json={"note": note})
    
    
