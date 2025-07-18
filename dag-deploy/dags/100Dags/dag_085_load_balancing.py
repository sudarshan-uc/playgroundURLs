from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_085_load_balancing', default_args={'owner': 'lb_85', 'start_date': datetime(2024, 6, 5)}, schedule_interval='@hourly', tags=['load_balancing'])
def balance_load(**context): time.sleep(random.uniform(5, 300)); return {'requests_balanced': random.randint(100, 10000)}
tasks = [PythonOperator(task_id=f'balance_{i:03d}', python_callable=balance_load, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
