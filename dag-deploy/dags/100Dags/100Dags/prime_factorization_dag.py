"""
Prime Number Factorization DAG - Number theory operations
"""
import random
import time
import math
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def prime_factorization_task(**context):
    """Factorize random numbers"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    factors_found = []
    
    while time.time() - start_time < target_time:
        num = random.randint(1000, 100000)
        factors = []
        d = 2
        while d * d <= num:
            while num % d == 0:
                factors.append(d)
                num //= d
            d += 1
        if num > 1:
            factors.append(num)
        factors_found.append({'number': num, 'factors': factors})
        time.sleep(0.01)
    
    result = {'numbers_factored': len(factors_found)}
    context['task_instance'].xcom_push(key='factorization_result', value=result)
    return result

def validate_factors(**context):
    """Validate factorization results"""
    time.sleep(random.uniform(10, 120))
    return {'validation_complete': True}

default_args = {
    'owner': 'number_theory_team',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'prime_factorization_dag',
    default_args=default_args,
    description='Prime number factorization workflow',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['math', 'prime', 'factorization'],
)

# Initial setup task
setup_task = BashOperator(
    task_id='setup_environment',
    bash_command='echo "Setting up prime factorization environment" && sleep 10',
    dag=dag,
)

# Prime factorization tasks
prime_tasks = []
for i in range(1, 8):
    task = PythonOperator(
        task_id=f'prime_factorize_{i}',
        python_callable=prime_factorization_task,
        dag=dag,
    )
    prime_tasks.append(task)

# Validation task
validation_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_factors,
    dag=dag,
)

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='echo "Cleaning up factorization results" && sleep 5',
    dag=dag,
)

# Set dependencies
setup_task >> prime_tasks[0]
for i in range(len(prime_tasks) - 1):
    prime_tasks[i] >> prime_tasks[i + 1]
prime_tasks[-1] >> validation_task >> cleanup_task
