from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'prime_numbers_003',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dag_003_prime_numbers',
    default_args=default_args,
    description='Prime number calculation DAG',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    concurrency=12,
    max_active_runs=2,
    tags=['prime', 'numbers', 'math'],
)

def find_primes(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    primes_found = []
    n = 2
    
    while time.time() - start_time < target_time:
        is_prime = True
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                is_prime = False
                break
        if is_prime:
            primes_found.append(n)
        n += 1
        time.sleep(0.001)
    
    return {'primes_count': len(primes_found)}

def check_prime(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    checks_done = 0
    
    while time.time() - start_time < target_time:
        n = random.randint(1, 10000)
        is_prime = True
        if n < 2:
            is_prime = False
        else:
            for i in range(2, int(n**0.5) + 1):
                if n % i == 0:
                    is_prime = False
                    break
        checks_done += 1
        time.sleep(0.01)
    
    return {'prime_checks': checks_done}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 2 == 0:
        task = PythonOperator(
            task_id=f'find_primes_{i:03d}',
            python_callable=find_primes,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'check_prime_{i:03d}',
            python_callable=check_prime,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
