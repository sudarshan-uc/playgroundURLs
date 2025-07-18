from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_070_content_moderation', default_args={'owner': 'moderator', 'start_date': datetime(2024, 4, 20)}, schedule_interval='@hourly', tags=['content'])
def moderate_content(**context): time.sleep(random.uniform(5, 300)); return {'content_moderated': random.randint(10, 100)}
tasks = [PythonOperator(task_id=f'mod_{i:03d}', python_callable=moderate_content, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
