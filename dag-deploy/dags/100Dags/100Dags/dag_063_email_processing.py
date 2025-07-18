from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_063_email_processing', default_args={'owner': 'email_admin', 'start_date': datetime(2024, 3, 10)}, schedule_interval='@hourly', tags=['email', 'processing'])

def parse_emails(**context):
    time.sleep(random.uniform(5, 300))
    return {'emails_parsed': random.randint(20, 200)}

def send_notifications(**context):
    time.sleep(random.uniform(5, 300))
    return {'notifications_sent': random.randint(5, 50)}

tasks = [PythonOperator(task_id=f'email_{i:03d}', python_callable=parse_emails if i%2==0 else send_notifications, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)):
    for dep in random.sample(tasks[:i], random.randint(1, min(4, i))):
        _ = dep >> tasks[i]
