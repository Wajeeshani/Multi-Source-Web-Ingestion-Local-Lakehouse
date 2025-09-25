from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="simple_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    start >> end