"""
File Processing DAG - File operations and transformations
"""
import random
import time
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def simulate_file_processing(**context):
    """Simulate file processing operations"""
    target_time = random.uniform(10, 200)
    start_time = time.time()
    files_processed = 0
    
    while time.time() - start_time < target_time:
        # Simulate file operations
        file_size = random.randint(1024, 1024*1024)  # 1KB to 1MB
        processing_time = file_size / (1024 * 100)  # Simulate processing speed
        time.sleep(min(processing_time, 2.0))
        files_processed += 1
    
    result = {'files_processed': files_processed}
    context['task_instance'].xcom_push(key='processing_result', value=result)
    return result

def file_validation(**context):
    """Validate processed files"""
    time.sleep(random.uniform(15, 90))
    task_instance = context['task_instance']
    processing_result = task_instance.xcom_pull(task_ids='process_files_1', key='processing_result')
    
    validation_passed = random.choice([True, True, True, False])  # 75% pass rate
    return {'validation_passed': validation_passed, 'files_validated': processing_result.get('files_processed', 0) if processing_result else 0}

default_args = {
    'owner': 'file_processing_team',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'file_processing_dag',
    default_args=default_args,
    description='File processing and validation workflow',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    max_active_runs=10,
    concurrency=128,
    tags=['file_processing', 'validation', 'etl'],
)

# File processing tasks
file_tasks = []
for i in range(1, 35):
    task = PythonOperator(
        task_id=f'process_files_{i}',
        python_callable=simulate_file_processing,
        dag=dag,
    )
    file_tasks.append(task)

# Validation task
validation = PythonOperator(
    task_id='validate_files',
    python_callable=file_validation,
    dag=dag,
)

# Cleanup
cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='echo "Cleaning up temporary files" && sleep 10',
    dag=dag,
)

# Set some dependencies
file_tasks[0] >> file_tasks[1] >> file_tasks[2] >> validation
file_tasks[3] >> file_tasks[4] >> validation
validation >> cleanup
