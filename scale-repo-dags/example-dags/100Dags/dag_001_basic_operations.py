from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'basic_operations_001',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_001_basic_operations',
    default_args=default_args,
    description='Basic mathematical operations DAG',
    schedule_interval='@daily',
    catchup=False,
    concurrency=16,
    max_active_runs=2,
    tags=['basic', 'math', 'operations'],
)

def add_numbers(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    additions_done = 0
    
    while time.time() - start_time < target_time:
        result = random.randint(1, 1000) + random.randint(1, 1000)
        additions_done += 1
        time.sleep(0.01)
    
    return {'additions_done': additions_done}

def multiply_numbers(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    multiplications_done = 0
    
    while time.time() - start_time < target_time:
        result = random.randint(1, 100) * random.randint(1, 100)
        multiplications_done += 1
        time.sleep(0.01)
    
    return {'multiplications_done': multiplications_done}

def divide_numbers(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    divisions_done = 0
    
    while time.time() - start_time < target_time:
        dividend = random.randint(100, 10000)
        divisor = random.randint(1, 100)
        result = dividend / divisor
        divisions_done += 1
        time.sleep(0.01)
    
    return {'divisions_done': divisions_done}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'add_numbers_{i:03d}',
            python_callable=add_numbers,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'multiply_numbers_{i:03d}',
            python_callable=multiply_numbers,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'divide_numbers_{i:03d}',
            python_callable=divide_numbers,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
