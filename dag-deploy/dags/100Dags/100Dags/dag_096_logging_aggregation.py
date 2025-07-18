from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_096_logging_aggregation', default_args={'owner': 'logging_96', 'start_date': datetime(2024, 6, 16)}, schedule_interval='@hourly', tags=['logging'])
def aggregate_logs(**context): time.sleep(random.uniform(5, 300)); return {'logs_aggregated': random.randint(1000, 10000)}
tasks = [PythonOperator(task_id=f'log_{i:03d}', python_callable=aggregate_logs, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
