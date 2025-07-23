from datetime import datetime, timedelta
import random
import time
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_validator',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 5),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=8),
}

dag = DAG(
    'dag_052_data_validation',
    default_args=default_args,
    description='Data validation and quality checks',
    schedule_interval='@weekly',
    catchup=True,
    concurrency=16,
    max_active_runs=2,
    tags=['data', 'validation', 'quality'],
)

def validate_schema(**context):
    """Validate data schema"""
    start_time = time.time()
    target_time = random.uniform(5, 300)
    
    schemas_validated = 0
    while time.time() - start_time < target_time:
        # Simulate schema validation
        schema = {'id': 'int', 'name': 'string', 'value': 'float'}
        _ = json.dumps(schema)
        schemas_validated += 1
        time.sleep(0.1)
    
    context['task_instance'].xcom_push(key='schemas_validated', value=schemas_validated)
    return {'schemas_validated': schemas_validated}

def check_data_quality(**context):
    """Check data quality metrics"""
    start_time = time.time()
    target_time = random.uniform(5, 300)
    
    quality_checks = 0
    while time.time() - start_time < target_time:
        # Simulate quality checks
        quality_score = random.uniform(0.7, 1.0)
        quality_checks += 1
        time.sleep(0.05)
    
    context['task_instance'].xcom_push(key='quality_checks', value=quality_checks)
    return {'quality_checks': quality_checks}

def validate_completeness(**context):
    """Validate data completeness"""
    start_time = time.time()
    target_time = random.uniform(5, 300)
    
    completeness_checks = 0
    while time.time() - start_time < target_time:
        # Simulate completeness validation
        completeness_rate = random.uniform(0.85, 1.0)
        completeness_checks += 1
        time.sleep(0.08)
    
    return {'completeness_checks': completeness_checks}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'validate_schema_{i:03d}',
            python_callable=validate_schema,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'check_quality_{i:03d}',
            python_callable=check_data_quality,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'validate_completeness_{i:03d}',
            python_callable=validate_completeness,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
