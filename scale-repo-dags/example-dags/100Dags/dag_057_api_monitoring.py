from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'api_monitor',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_057_api_monitoring',
    default_args=default_args,
    description='API monitoring and health checks',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    concurrency=64,
    max_active_runs=5,
    tags=['api', 'monitoring', 'health'],
)

def check_api_health(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    api_checks = 0
    while time.time() - start_time < target_time:
        response_time = random.uniform(100, 2000)
        api_checks += 1
        time.sleep(0.05)
    context['task_instance'].xcom_push(key='api_checks', value=api_checks)
    return {'api_checks': api_checks}

def monitor_latency(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    latency_measurements = 0
    while time.time() - start_time < target_time:
        latency = random.uniform(50, 500)
        latency_measurements += 1
        time.sleep(0.03)
    return {'latency_measurements': latency_measurements}

def check_endpoints(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    endpoints_checked = 0
    while time.time() - start_time < target_time:
        status_code = random.choice([200, 201, 404, 500])
        endpoints_checked += 1
        time.sleep(0.08)
    return {'endpoints_checked': endpoints_checked}

num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'check_health_{i:03d}',
            python_callable=check_api_health,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'monitor_latency_{i:03d}',
            python_callable=monitor_latency,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'check_endpoints_{i:03d}',
            python_callable=check_endpoints,
            dag=dag,
        )
    tasks.append(task)

for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
