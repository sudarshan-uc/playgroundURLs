from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'stream_processor_72',
    'depends_on_past': True,
    'start_date': datetime(2024, 5, 2),
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_072_stream_processing',
    default_args=default_args,
    description='Real-time stream processing DAG',
    schedule_interval=timedelta(minutes=15),
    catchup=True,
    concurrency=64,
    max_active_runs=5,
    tags=['stream', 'realtime', 'processing'],
)

def consume_stream(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    messages_consumed = 0
    
    while time.time() - start_time < target_time:
        messages_consumed += random.randint(1, 50)
        time.sleep(0.05)
    
    return {'messages_consumed': messages_consumed}

def process_stream_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    records_processed = 0
    
    while time.time() - start_time < target_time:
        records_processed += random.randint(1, 20)
        time.sleep(0.08)
    
    return {'records_processed': records_processed}

def publish_results(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    results_published = 0
    
    while time.time() - start_time < target_time:
        results_published += 1
        time.sleep(0.12)
    
    return {'results_published': results_published}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'consume_stream_{i:03d}',
            python_callable=consume_stream,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'process_stream_{i:03d}',
            python_callable=process_stream_data,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'publish_results_{i:03d}',
            python_callable=publish_results,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
