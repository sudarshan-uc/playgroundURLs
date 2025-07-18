from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_092_infrastructure_monitoring', default_args={'owner': 'infra_92', 'start_date': datetime(2024, 6, 12)}, schedule_interval='@hourly', tags=['infrastructure'])
def monitor_infra(**context): time.sleep(random.uniform(5, 300)); return {'metrics_collected': random.randint(100, 1000)}
tasks = [PythonOperator(task_id=f'infra_{i:03d}', python_callable=monitor_infra, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
