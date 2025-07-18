from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_060_network_scan', default_args={'owner': 'net_admin', 'start_date': datetime(2024, 2, 15), 'retries': 2}, schedule_interval='@hourly', tags=['network', 'scan'])
def scan_ports(**context): time.sleep(random.uniform(5, 300)); return {'ports_scanned': random.randint(100, 1000)}
def check_connectivity(**context): time.sleep(random.uniform(5, 300)); return {'connections_checked': random.randint(50, 500)}
tasks = [PythonOperator(task_id=f'scan_{i:03d}', python_callable=scan_ports if i%2==0 else check_connectivity, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
