from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'fibonacci_005',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    'dag_005_fibonacci_sequence',
    default_args=default_args,
    description='Fibonacci sequence calculation DAG',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    concurrency=6,
    max_active_runs=3,
    tags=['fibonacci', 'sequence', 'recursive'],
)

def calculate_fibonacci(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    fibonacci_numbers = []
    a, b = 0, 1
    
    while time.time() - start_time < target_time:
        fibonacci_numbers.append(a)
        a, b = b, a + b
        time.sleep(0.001)
        
        # Reset if getting too large
        if a > 1000000:
            a, b = 0, 1
    
    return {'fibonacci_count': len(fibonacci_numbers)}

def fibonacci_golden_ratio(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    ratios_calculated = 0
    
    while time.time() - start_time < target_time:
        a, b = 1, 1
        for _ in range(random.randint(10, 50)):
            a, b = b, a + b
        
        if a != 0:
            ratio = b / a
            ratios_calculated += 1
        
        time.sleep(0.01)
    
    return {'golden_ratios': ratios_calculated}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 2 == 0:
        task = PythonOperator(
            task_id=f'calculate_fibonacci_{i:03d}',
            python_callable=calculate_fibonacci,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'fibonacci_golden_ratio_{i:03d}',
            python_callable=fibonacci_golden_ratio,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
