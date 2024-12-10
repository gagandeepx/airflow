from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Tejaswini',
    'depends_on_past':False,
    'start_date':datetime(2023,7,20),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG('scenario_1',
  schedule_interval = None,
  default_args = default_args,
  description='DAG to trigger the existing workflow on Databricks',
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_default',
    job_id = 1116373861050779
  )

