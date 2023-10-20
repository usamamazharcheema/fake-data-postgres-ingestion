from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from main import log_into_csv, ingest_into_db

default_args = {
    'owner' : 'Usama',
    'retries': 5,
    'retry_delay': timedelta(minutes = 2) 
}

with DAG( 
    dag_id = 'functions_insertion',
    default_args = default_args,
    description = 'Insert functions into csv/db',
    start_date = datetime(2023, 10, 19, 11, 19),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'log_into_csv',
        python_callable= log_into_csv
    )
    task2 = PythonOperator(
        task_id = 'ingest_into_db',
        python_callable= ingest_into_db
    )

    task1.set_downstream(task2)