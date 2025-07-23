from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_090_auto_scaling', default_args={'owner': 'scaling_90', 'start_date': datetime(2024, 6, 10)}, schedule_interval='@hourly', tags=['scaling'])
def auto_scale(**context): time.sleep(random.uniform(5, 300)); return {'instances_scaled': random.randint(1, 20)}
tasks = [PythonOperator(task_id=f'scale_{i:03d}', python_callable=auto_scale, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
