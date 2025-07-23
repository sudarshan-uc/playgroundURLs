from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_097_metrics_collection', default_args={'owner': 'metrics_97', 'start_date': datetime(2024, 6, 17)}, schedule_interval='@daily', tags=['metrics'])
def collect_metrics(**context): time.sleep(random.uniform(5, 300)); return {'metrics_collected': random.randint(100, 5000)}
tasks = [PythonOperator(task_id=f'metric_{i:03d}', python_callable=collect_metrics, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
