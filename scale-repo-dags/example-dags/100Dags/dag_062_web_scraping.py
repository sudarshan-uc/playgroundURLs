from datetime import datetime
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('dag_062_web_scraping', default_args={'owner': 'scraper', 'start_date': datetime(2024, 3, 5)}, schedule_interval='@weekly', tags=['scraping', 'web'])

def scrape_data(**context):
    time.sleep(random.uniform(5, 300))
    return {'pages_scraped': random.randint(10, 100)}

def process_html(**context):
    time.sleep(random.uniform(5, 300))
    return {'html_processed': random.randint(5, 50)}

tasks = [PythonOperator(task_id=f'scrape_{i:03d}', python_callable=scrape_data if i%2==0 else process_html, dag=dag) for i in range(random.randint(2, 100))]
for i in range(1, len(tasks)):
    for dep in random.sample(tasks[:i], random.randint(1, min(4, i))):
        _ = dep >> tasks[i]
