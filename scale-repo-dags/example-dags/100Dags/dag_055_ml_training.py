from datetime import datetime, timedelta
import random
import time
import math
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ml_engineer',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'dag_055_ml_training',
    default_args=default_args,
    description='Machine learning model training pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=True,
    concurrency=16,
    max_active_runs=1,
    tags=['ml', 'training', 'models'],
)

def prepare_features(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    features_prepared = 0
    while time.time() - start_time < target_time:
        # Simulate feature engineering
        _ = math.sqrt(random.uniform(1, 1000))
        features_prepared += 1
        time.sleep(0.1)
    return {'features_prepared': features_prepared}

def train_model(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    epochs_completed = 0
    while time.time() - start_time < target_time:
        # Simulate model training
        loss = random.uniform(0.1, 1.0)
        epochs_completed += 1
        time.sleep(0.2)
    return {'epochs_completed': epochs_completed}

def validate_model(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    validations_done = 0
    while time.time() - start_time < target_time:
        # Simulate model validation
        accuracy = random.uniform(0.7, 0.95)
        validations_done += 1
        time.sleep(0.15)
    return {'validations_done': validations_done}

num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'prepare_features_{i:03d}',
            python_callable=prepare_features,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'train_model_{i:03d}',
            python_callable=train_model,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'validate_model_{i:03d}',
            python_callable=validate_model,
            dag=dag,
        )
    tasks.append(task)

for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
