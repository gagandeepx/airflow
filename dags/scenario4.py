from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 10),
    'email': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'SCENARIO_4',
    default_args=default_args,
    description='DAG to submit a notebook from DBFS on Databricks',
    schedule_interval=None
)
params={'hi':'test','ab':'zx'}
spark_python_task = DatabricksSubmitRunOperator(
    task_id='test_cl',
    databricks_conn_id = 'databricks_default',
    new_cluster={
        'spark_version': '12.2.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 1,
        'custom_tags': {
                        "Env": "Dev"
                    },

    },
    json={'spark_python_task':{'python_file': 'dbfs:/FileStore/TestFile4.py','parameters': [str(params)] }

    }
    ,
    dag=dag
)