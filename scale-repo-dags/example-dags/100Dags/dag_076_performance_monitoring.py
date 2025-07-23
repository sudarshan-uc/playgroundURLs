from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_076_performance_monitoring', default_args={'owner': 'perf_76', 'start_date': datetime(2024, 5, 6)}, schedule_interval='@hourly', tags=['performance'])
def monitor_cpu(**context): time.sleep(random.uniform(5, 300)); return {'cpu_monitored': True}
def monitor_memory(**context): time.sleep(random.uniform(5, 300)); return {'memory_monitored': True}
tasks = [PythonOperator(task_id=f'perf_{i:03d}', python_callable=monitor_cpu if i%2==0 else monitor_memory, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
