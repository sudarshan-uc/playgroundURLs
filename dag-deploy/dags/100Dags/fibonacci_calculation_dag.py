"""
Fibonacci Calculation DAG - Mathematical computation workflow
"""
import random
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def fibonacci_task(**context):
    """Calculate Fibonacci numbers for random duration"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    a, b = 0, 1
    count = 0
    while time.time() - start_time < target_time:
        a, b = b, a + b
        count += 1
        if count % 1000 == 0:
            time.sleep(0.001)
    
    result = {'fibonacci_count': count, 'last_fib': str(b)[:100]}
    context['task_instance'].xcom_push(key='fibonacci_result', value=result)
    return result

def fibonacci_aggregator(**context):
    """Aggregate results from fibonacci tasks"""
    time.sleep(random.uniform(5, 60))
    task_instance = context['task_instance']
    results = []
    for i in range(1, 6):
        result = task_instance.xcom_pull(task_ids=f'fibonacci_calc_{i}', key='fibonacci_result')
        if result:
            results.append(result)
    return {'total_calculations': len(results)}

default_args = {
    'owner': 'math_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fibonacci_calculation_dag',
    default_args=default_args,
    description='Fibonacci mathematical computation workflow',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    max_active_runs=2,
    tags=['math', 'fibonacci', 'computation'],
)

# Create 5 fibonacci calculation tasks
fibonacci_tasks = []
for i in range(1, 6):
    task = PythonOperator(
        task_id=f'fibonacci_calc_{i}',
        python_callable=fibonacci_task,
        dag=dag,
    )
    fibonacci_tasks.append(task)

# Create aggregator task
aggregator = PythonOperator(
    task_id='fibonacci_aggregator',
    python_callable=fibonacci_aggregator,
    dag=dag,
)

# Set dependencies
for task in fibonacci_tasks:
    task >> aggregator
