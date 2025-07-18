from datetime import datetime, timedelta
import random
import time
import math
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'computation_73',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 3),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'dag_073_mathematical_computation',
    default_args=default_args,
    description='Complex mathematical computations',
    schedule_interval='@daily',
    catchup=False,
    concurrency=16,
    max_active_runs=1,
    tags=['math', 'computation', 'complex'],
)

def calculate_primes(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    primes_found = 0
    
    while time.time() - start_time < target_time:
        n = random.randint(100, 10000)
        is_prime = True
        for i in range(2, int(math.sqrt(n)) + 1):
            if n % i == 0:
                is_prime = False
                break
        if is_prime:
            primes_found += 1
        time.sleep(0.01)
    
    return {'primes_found': primes_found}

def matrix_operations(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    operations_completed = 0
    
    while time.time() - start_time < target_time:
        size = random.randint(5, 20)
        matrix = [[random.random() for _ in range(size)] for _ in range(size)]
        # Calculate determinant (simplified)
        _ = sum(row[0] for row in matrix)
        operations_completed += 1
        time.sleep(0.05)
    
    return {'operations_completed': operations_completed}

def statistical_analysis(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    analyses_done = 0
    
    while time.time() - start_time < target_time:
        data = [random.random() for _ in range(1000)]
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        analyses_done += 1
        time.sleep(0.1)
    
    return {'analyses_done': analyses_done}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'calculate_primes_{i:03d}',
            python_callable=calculate_primes,
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'matrix_ops_{i:03d}',
            python_callable=matrix_operations,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'statistical_analysis_{i:03d}',
            python_callable=statistical_analysis,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
