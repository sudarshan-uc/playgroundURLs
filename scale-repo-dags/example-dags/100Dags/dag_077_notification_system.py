from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_077_notification_system', default_args={'owner': 'notify_77', 'start_date': datetime(2024, 5, 7)}, schedule_interval='@daily', tags=['notifications'])
def send_email(**context): time.sleep(random.uniform(5, 300)); return {'emails_sent': random.randint(1, 100)}
def send_sms(**context): time.sleep(random.uniform(5, 300)); return {'sms_sent': random.randint(1, 50)}
tasks = [PythonOperator(task_id=f'notify_{i:03d}', python_callable=send_email if i%2==0 else send_sms, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
