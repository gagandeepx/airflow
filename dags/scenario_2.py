from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Tejaswini',
    'depends_on_past':False,
    'start_date':datetime(2023,7,20),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG('scenario_2',
  schedule_interval = None,
  default_args = default_args,
  description='DAG to run a notebook with parameters',
  ) as dag:
  opr_run_now = DatabricksSubmitRunOperator(
    task_id='run_now',
    databricks_conn_id = 'databricks_default',
    existing_cluster_id='0710-102959-lnua2g2o',
    notebook_task={
        'notebook_path': 'dbfs:/FileStore/scenario2.ipynb',
        'notebook_params':{
          'Name':'Databricks Team',
          'Greeting':'Welcome all!'
        }
    }
)

