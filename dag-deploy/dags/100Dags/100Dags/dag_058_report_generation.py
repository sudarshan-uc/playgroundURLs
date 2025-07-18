from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'report_generator',
    'depends_on_past': True,
    'start_date': datetime(2024, 2, 5),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=12),
}

dag = DAG(
    'dag_058_report_generation',
    default_args=default_args,
    description='Automated report generation system',
    schedule_interval='@daily',
    catchup=True,
    concurrency=24,
    max_active_runs=3,
    tags=['reports', 'generation', 'analytics'],
)

def collect_metrics(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    metrics_collected = 0
    while time.time() - start_time < target_time:
        metrics_collected += random.randint(1, 20)
        time.sleep(0.1)
    return {'metrics_collected': metrics_collected}

def generate_charts(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    charts_generated = 0
    while time.time() - start_time < target_time:
        charts_generated += 1
        time.sleep(0.2)
    return {'charts_generated': charts_generated}

def compile_report(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    reports_compiled = 0
    while time.time() - start_time < target_time:
        reports_compiled += 1
        time.sleep(0.25)
    return {'reports_compiled': reports_compiled}

num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'collect_metrics_{i:03d}',
            python_callable=collect_metrics,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'generate_charts_{i:03d}',
            python_callable=generate_charts,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'compile_report_{i:03d}',
            python_callable=compile_report,
            dag=dag,
        )
    tasks.append(task)

for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
