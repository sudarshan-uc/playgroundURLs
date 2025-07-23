from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_080_compliance_check', default_args={'owner': 'compliance_80', 'start_date': datetime(2024, 5, 10)}, schedule_interval='@monthly', tags=['compliance'])
def check_compliance(**context): time.sleep(random.uniform(5, 300)); return {'checks_done': random.randint(1, 50)}
tasks = [PythonOperator(task_id=f'compliance_{i:03d}', python_callable=check_compliance, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
