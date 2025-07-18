from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'dag_054_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data processing',
    schedule_interval='@daily',
    catchup=False,
    concurrency=32,
    max_active_runs=2,
    tags=['etl', 'pipeline', 'data'],
)

def extract_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_extracted = 0
    while time.time() - start_time < target_time:
        records_extracted += random.randint(1, 100)
        time.sleep(0.1)
    return {'records_extracted': records_extracted}

def transform_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_transformed = 0
    while time.time() - start_time < target_time:
        records_transformed += random.randint(1, 50)
        time.sleep(0.08)
    return {'records_transformed': records_transformed}

def load_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_loaded = 0
    while time.time() - start_time < target_time:
        records_loaded += random.randint(1, 30)
        time.sleep(0.12)
    return {'records_loaded': records_loaded}

num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'extract_data_{i:03d}',
            python_callable=extract_data,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'transform_data_{i:03d}',
            python_callable=transform_data,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'load_data_{i:03d}',
            python_callable=load_data,
            dag=dag,
        )
    tasks.append(task)

for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
