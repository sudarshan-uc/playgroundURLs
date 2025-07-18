from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_061_system_health', default_args={'owner': 'health_monitor', 'start_date': datetime(2024, 3, 1)}, schedule_interval='@daily', tags=['health', 'system'])

def check_cpu(**context):
    time.sleep(random.uniform(5, 300))
    return {'cpu_usage': random.uniform(10, 90)}

def check_memory(**context):
    time.sleep(random.uniform(5, 300))
    return {'memory_usage': random.uniform(20, 80)}

def check_disk(**context):
    time.sleep(random.uniform(5, 300))
    return {'disk_usage': random.uniform(30, 95)}

tasks = []
num_tasks = random.randint(2, 100)
for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(task_id=f'cpu_check_{i:03d}', python_callable=check_cpu, dag=dag)
    elif i % 3 == 1:
        task = PythonOperator(task_id=f'memory_check_{i:03d}', python_callable=check_memory, dag=dag)
    else:
        task = PythonOperator(task_id=f'disk_check_{i:03d}', python_callable=check_disk, dag=dag)
    tasks.append(task)

for i in range(1, len(tasks)):
    deps = random.sample(tasks[:i], random.randint(1, min(4, i)))
    for dep in deps:
        _ = dep >> tasks[i]
