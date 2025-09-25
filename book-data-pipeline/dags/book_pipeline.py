from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_pipeline():
    print("Book pipeline test - working!")
    return "Pipeline test passed"

with DAG(
    dag_id='book_data_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
    },
    description='Book data ingestion pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['books'],
) as dag:

    test_task = PythonOperator(
        task_id='test_pipeline',
        python_callable=test_pipeline,
    )