from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'batch_processor_71',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=8),
}

dag = DAG(
    'dag_071_batch_processing',
    default_args=default_args,
    description='Batch processing pipeline with parallel tasks',
    schedule_interval=timedelta(hours=3),
    catchup=False,
    concurrency=32,
    max_active_runs=2,
    tags=['batch', 'processing', 'parallel'],
)

def process_batch_data(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    batches_processed = 0
    
    while time.time() - start_time < target_time:
        # Simulate batch processing
        batch_size = random.randint(100, 1000)
        batches_processed += 1
        time.sleep(0.1)
    
    context['task_instance'].xcom_push(key='batches_processed', value=batches_processed)
    return {'batches_processed': batches_processed}

def validate_batch_results(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    validations_done = 0
    
    while time.time() - start_time < target_time:
        # Simulate validation
        validation_result = random.choice([True, False])
        validations_done += 1
        time.sleep(0.15)
    
    return {'validations_done': validations_done}

def archive_batch(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    archives_created = 0
    
    while time.time() - start_time < target_time:
        # Simulate archiving
        archive_size = random.uniform(1.0, 100.0)  # MB
        archives_created += 1
        time.sleep(0.2)
    
    return {'archives_created': archives_created}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'process_batch_{i:03d}',
            python_callable=process_batch_data,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'validate_batch_{i:03d}',
            python_callable=validate_batch_results,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'archive_batch_{i:03d}',
            python_callable=archive_batch,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
