from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_083_cloud_migration', default_args={'owner': 'cloud_83', 'start_date': datetime(2024, 6, 3)}, schedule_interval='@daily', tags=['cloud', 'migration'])
def migrate_data(**context): time.sleep(random.uniform(5, 300)); return {'data_migrated': random.randint(1, 1000)}
tasks = [PythonOperator(task_id=f'migrate_{i:03d}', python_callable=migrate_data, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
