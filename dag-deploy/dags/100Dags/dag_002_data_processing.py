from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_processing_002',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'dag_002_data_processing',
    default_args=default_args,
    description='Data processing operations DAG',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=8,
    max_active_runs=1,
    tags=['data', 'processing', 'statistics'],
)

def process_dataset(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_processed = 0
    
    while time.time() - start_time < target_time:
        # Simulate processing a record
        data = [random.uniform(0, 100) for _ in range(random.randint(10, 1000))]
        mean_value = sum(data) / len(data)
        records_processed += 1
        time.sleep(0.01)
    
    return {'records_processed': records_processed}

def calculate_variance(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    calculations_done = 0
    
    while time.time() - start_time < target_time:
        data = [random.uniform(0, 100) for _ in range(random.randint(50, 500))]
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        calculations_done += 1
        time.sleep(0.01)
    
    return {'variance_calculations': calculations_done}

def sort_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    sorts_done = 0
    
    while time.time() - start_time < target_time:
        data = [random.randint(1, 10000) for _ in range(random.randint(100, 5000))]
        sorted_data = sorted(data)
        sorts_done += 1
        time.sleep(0.01)
    
    return {'sorts_completed': sorts_done}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'process_dataset_{i:03d}',
            python_callable=process_dataset,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'calculate_variance_{i:03d}',
            python_callable=calculate_variance,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'sort_data_{i:03d}',
            python_callable=sort_data,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
