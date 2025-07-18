from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'backup_admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 25),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=20),
}

dag = DAG(
    'dag_056_backup_system',
    default_args=default_args,
    description='System backup and recovery operations',
    schedule_interval='@weekly',
    catchup=False,
    concurrency=8,
    max_active_runs=1,
    tags=['backup', 'system', 'recovery'],
)

def backup_database(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    databases_backed_up = 0
    while time.time() - start_time < target_time:
        databases_backed_up += 1
        time.sleep(0.2)
    return {'databases_backed_up': databases_backed_up}

def backup_files(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    files_backed_up = 0
    while time.time() - start_time < target_time:
        files_backed_up += random.randint(1, 10)
        time.sleep(0.1)
    return {'files_backed_up': files_backed_up}

def verify_backup(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    verifications_done = 0
    while time.time() - start_time < target_time:
        verifications_done += 1
        time.sleep(0.15)
    return {'verifications_done': verifications_done}

num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'backup_database_{i:03d}',
            python_callable=backup_database,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'backup_files_{i:03d}',
            python_callable=backup_files,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'verify_backup_{i:03d}',
            python_callable=verify_backup,
            dag=dag,
        )
    tasks.append(task)

for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
