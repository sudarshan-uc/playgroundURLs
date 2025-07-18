"""
Matrix Multiplication DAG - Linear algebra computations
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def matrix_multiply_task(**context):
    """Multiply random matrices"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    multiplications = 0
    
    while time.time() - start_time < target_time:
        size = random.randint(10, 50)
        matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
        matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]
        
        result_matrix = [[0.0 for _ in range(size)] for _ in range(size)]
        for i in range(size):
            for j in range(size):
                for k in range(size):
                    result_matrix[i][j] += matrix_a[i][k] * matrix_b[k][j]
        multiplications += 1
    
    result = {'matrix_multiplications': multiplications}
    context['task_instance'].xcom_push(key='matrix_result', value=result)
    return result

def collect_matrix_results(**context):
    """Collect and aggregate matrix multiplication results"""
    time.sleep(random.uniform(15, 90))
    task_instance = context['task_instance']
    total_ops = 0
    
    for i in range(1, 12):
        result = task_instance.xcom_pull(task_ids=f'matrix_multiply_{i}', key='matrix_result')
        if result:
            total_ops += result.get('matrix_multiplications', 0)
    
    return {'total_matrix_operations': total_ops}

default_args = {
    'owner': 'linear_algebra_team',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'matrix_multiplication_dag',
    default_args=default_args,
    description='Matrix multiplication linear algebra workflow',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    max_active_runs=3,
    concurrency=32,
    tags=['math', 'matrix', 'linear_algebra'],
)

# Create matrix multiplication tasks
matrix_tasks = []
for i in range(1, 12):
    task = PythonOperator(
        task_id=f'matrix_multiply_{i}',
        python_callable=matrix_multiply_task,
        dag=dag,
    )
    matrix_tasks.append(task)

# Results collector
collector = PythonOperator(
    task_id='collect_results',
    python_callable=collect_matrix_results,
    dag=dag,
)

# Set dependencies - parallel execution then collect
for task in matrix_tasks:
    task >> collector
