from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_069_financial_reporting', default_args={'owner': 'finance', 'start_date': datetime(2024, 4, 15)}, schedule_interval='@monthly', tags=['finance'])
def calculate_revenue(**context): time.sleep(random.uniform(5, 300)); return {'revenue_calculated': True}
tasks = [PythonOperator(task_id=f'fin_{i:03d}', python_callable=calculate_revenue, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
