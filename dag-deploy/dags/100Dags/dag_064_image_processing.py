from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_064_image_processing', default_args={'owner': 'img_processor', 'start_date': datetime(2024, 3, 15)}, schedule_interval='@daily', tags=['image', 'processing'])
def resize_images(**context): time.sleep(random.uniform(5, 300)); return {'images_resized': random.randint(10, 100)}
def apply_filters(**context): time.sleep(random.uniform(5, 300)); return {'filters_applied': random.randint(5, 50)}
tasks = [PythonOperator(task_id=f'img_{i:03d}', python_callable=resize_images if i%2==0 else apply_filters, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)): [dep >> tasks[i] for dep in random.sample(tasks[:i], random.randint(1, min(4, i)))]
