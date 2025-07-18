from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    'dag_059_cache_management',
    default_args={'owner': 'cache_admin', 'start_date': datetime(2024, 2, 10), 'retries': 1},
    description='Cache management and optimization',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['cache', 'optimization'],
)

def clear_cache(**context):
    time.sleep(random.uniform(5, 300))
    return {'cache_cleared': True}

def warm_cache(**context):
    time.sleep(random.uniform(5, 300))
    return {'cache_warmed': True}

num_tasks = random.randint(2, 100)
tasks = []
for i in range(num_tasks):
    func = clear_cache if i % 2 == 0 else warm_cache
    task = PythonOperator(task_id=f'cache_task_{i:03d}', python_callable=func, dag=dag)
    tasks.append(task)

for i in range(1, len(tasks)):
    for dep in random.sample(tasks[:i], random.randint(1, min(4, i))):
        _ = dep >> tasks[i]
