from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_074_data_transformation', default_args={'owner': 'transform_74', 'start_date': datetime(2024, 5, 4), 'retries': 2}, schedule_interval='@hourly', tags=['transform', 'data'])

def transform_json(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_transformed = 0
    while time.time() - start_time < target_time:
        records_transformed += random.randint(1, 100)
        time.sleep(0.1)
    return {'records_transformed': records_transformed}

def clean_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_cleaned = 0
    while time.time() - start_time < target_time:
        records_cleaned += random.randint(1, 50)
        time.sleep(0.15)
    return {'records_cleaned': records_cleaned}

num_tasks = random.randint(2, 100)
tasks = []
for i in range(num_tasks):
    func = transform_json if i % 2 == 0 else clean_data
    task = PythonOperator(task_id=f'transform_task_{i:03d}', python_callable=func, dag=dag)
    tasks.append(task)

for i in range(1, len(tasks)):
    for dep in random.sample(tasks[:i], random.randint(1, min(4, i))):
        _ = dep >> tasks[i]
