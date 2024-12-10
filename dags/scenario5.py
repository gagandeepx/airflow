from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 13),
    'email': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'SCENARIO_5',
    default_args=default_args,
    description='DAG to submit a notebook from DBFS on Databricks',
    schedule_interval=None,
    tags=["Tags_test"]
)
params={'hi':'test','ab':'zx'}
params_1={'schema_name':'SN','table_name':'TN'}
task={'domain_name':'DN'}
spark_python_task = DatabricksSubmitRunOperator(
    task_id='test_cl',
    databricks_conn_id = 'databricks_default',
    new_cluster={
        'spark_version': '12.2.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 1,
        'custom_tags': {
                        "table_name": params_1["schema_name"] + '.' + params_1["table_name"],
                        "domain_name": task["domain_name"]
                    },

    },
    json = {'spark_python_task':{'python_file': 'dbfs:/FileStore/TestFile4.py','parameters': [str(params)] }
    }
    ,
    libraries = [
        {
            "pypi": {"package": "JayDeBeApi"}
            },
             {
            "pypi": {"package": "PyHive"}
            },
            {
            "pypi": {"package": "azure-storage-blob"}
            },
] ,
    dag=dag
)