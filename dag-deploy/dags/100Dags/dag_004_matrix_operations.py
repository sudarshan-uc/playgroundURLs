from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'matrix_operations_004',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dag_004_matrix_operations',
    default_args=default_args,
    description='Matrix mathematical operations DAG',
    schedule_interval='@weekly',
    catchup=False,
    concurrency=20,
    max_active_runs=1,
    tags=['matrix', 'linear_algebra', 'operations'],
)

def matrix_multiply(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    operations_done = 0
    
    while time.time() - start_time < target_time:
        size = random.randint(3, 50)
        matrix_a = [[random.uniform(-10, 10) for _ in range(size)] for _ in range(size)]
        matrix_b = [[random.uniform(-10, 10) for _ in range(size)] for _ in range(size)]
        
        # Matrix multiplication
        result = [[0 for _ in range(size)] for _ in range(size)]
        for i in range(size):
            for j in range(size):
                for k in range(size):
                    result[i][j] += matrix_a[i][k] * matrix_b[k][j]
        
        operations_done += 1
        time.sleep(0.05)
    
    return {'matrix_multiplications': operations_done}

def matrix_determinant(**context):
    start_time = time.time()
    target_time = random.uniform(5, 300)
    determinants_calculated = 0
    
    while time.time() - start_time < target_time:
        size = random.randint(2, 10)
        matrix = [[random.uniform(-10, 10) for _ in range(size)] for _ in range(size)]
        
        # Simple determinant calculation for 2x2
        if size == 2:
            det = matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]
        else:
            det = random.uniform(-1000, 1000)  # Simplified for larger matrices
        
        determinants_calculated += 1
        time.sleep(0.02)
    
    return {'determinants_calculated': determinants_calculated}

# Create tasks
num_tasks = random.randint(2, 100)
tasks = []

for i in range(num_tasks):
    if i % 2 == 0:
        task = PythonOperator(
            task_id=f'matrix_multiply_{i:03d}',
            python_callable=matrix_multiply,
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'matrix_determinant_{i:03d}',
            python_callable=matrix_determinant,
            dag=dag,
        )
    
    tasks.append(task)

# Set dependencies
for i in range(1, len(tasks)):
    num_deps = random.randint(1, min(4, i))
    deps = random.sample(tasks[:i], num_deps)
    for dep in deps:
        _ = dep >> tasks[i]
