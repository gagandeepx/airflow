from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 6),
    'email': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'SCENARIO_3',
    default_args=default_args,
    description='DAG to submit a notebook from DBFS on Databricks',
    schedule_interval=None,
)

spark_python_task = DatabricksSubmitRunOperator(
    task_id='test_cl',
    databricks_conn_id = 'databricks_default',
    new_cluster={
        'spark_version': '12.2.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'num_workers': 1
    },
    
    spark_python_task={
        "python_file": "dbfs:/FileStore/TestFIle.py",
        "base_parameters":
            {
            'Text': 'khelshankar'
        }
    },
    dag=dag
)