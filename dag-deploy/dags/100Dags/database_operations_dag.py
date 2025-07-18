"""
Database Operations DAG - Simulated database queries
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def simulate_database_query(**context):
    """Simulate database query operations"""
    query_types = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'JOIN']
    query_type = random.choice(query_types)
    
    # Different execution times for different query types
    if query_type == 'SELECT':
        execution_time = random.uniform(5, 60)
    elif query_type == 'INSERT':
        execution_time = random.uniform(10, 120)
    elif query_type == 'UPDATE':
        execution_time = random.uniform(15, 180)
    elif query_type == 'DELETE':
        execution_time = random.uniform(20, 200)
    else:  # JOIN
        execution_time = random.uniform(30, 300)
    
    time.sleep(execution_time)
    
    result = {
        'query_type': query_type,
        'execution_time': execution_time,
        'rows_affected': random.randint(1, 10000)
    }
    context['task_instance'].xcom_push(key='query_result', value=result)
    return result

default_args = {
    'owner': 'database_team',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'database_operations_dag',
    default_args=default_args,
    description='Database operations and query execution',
    schedule_interval=timedelta(hours=2),
    catchup=True,
    max_active_runs=3,
    concurrency=64,
    tags=['database', 'queries', 'sql'],
)

# Database connection setup
setup_db = BashOperator(
    task_id='setup_database_connection',
    bash_command='echo "Setting up database connection" && sleep 15',
    dag=dag,
)

# Query tasks
query_tasks = []
for i in range(1, 28):
    task = PythonOperator(
        task_id=f'execute_query_{i}',
        python_callable=simulate_database_query,
        dag=dag,
    )
    query_tasks.append(task)

# Database maintenance
maintenance = BashOperator(
    task_id='database_maintenance',
    bash_command='echo "Running database maintenance" && sleep 45',
    dag=dag,
)

# Set dependencies
setup_db >> query_tasks[0]
for i in range(len(query_tasks) - 1):
    if i % 3 == 0:  # Every 3rd task creates a chain
        query_tasks[i] >> query_tasks[i + 1]

query_tasks[-1] >> maintenance
