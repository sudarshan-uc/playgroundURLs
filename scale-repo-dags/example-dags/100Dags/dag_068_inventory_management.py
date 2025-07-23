from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_068_inventory_management', default_args={'owner': 'inventory', 'start_date': datetime(2024, 4, 10)}, schedule_interval='@daily', tags=['inventory'])
def update_stock(**context): time.sleep(random.uniform(5, 300)); return {'stock_updated': random.randint(10, 100)}
tasks = [PythonOperator(task_id=f'inv_{i:03d}', python_callable=update_stock, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
