"""
Web Scraping Simulation DAG - HTTP operations
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def simulate_web_scraping(**context):
    """Simulate web scraping operations"""
    target_time = random.uniform(10, 200)
    start_time = time.time()
    pages_scraped = 0
    
    while time.time() - start_time < target_time:
        # Simulate HTTP request delay
        time.sleep(random.uniform(0.1, 2.0))
        pages_scraped += 1
        
        # Simulate processing
        for _ in range(random.randint(100, 1000)):
            _ = hash(str(random.random()))
    
    result = {'pages_scraped': pages_scraped, 'duration': time.time() - start_time}
    context['task_instance'].xcom_push(key='scraping_result', value=result)
    return result

default_args = {
    'owner': 'web_scraping_team',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'web_scraping_simulation_dag',
    default_args=default_args,
    description='Web scraping simulation workflow',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=5,
    concurrency=64,
    tags=['web_scraping', 'http', 'simulation'],
)

# Setup task
setup = BashOperator(
    task_id='setup_scraping',
    bash_command='echo "Setting up web scraping environment" && sleep 5',
    dag=dag,
)

# Scraping tasks
scraping_tasks = []
for i in range(1, 23):
    task = PythonOperator(
        task_id=f'scrape_site_{i}',
        python_callable=simulate_web_scraping,
        dag=dag,
    )
    scraping_tasks.append(task)

# Create branched dependencies
setup >> scraping_tasks[0]
setup >> scraping_tasks[1]
scraping_tasks[0] >> scraping_tasks[2] >> scraping_tasks[5]
scraping_tasks[1] >> scraping_tasks[3] >> scraping_tasks[6]
scraping_tasks[2] >> scraping_tasks[4]
