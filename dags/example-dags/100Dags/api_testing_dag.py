"""
API Testing DAG - REST API endpoint testing
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def test_api_endpoint(**context):
    """Test API endpoint responses"""
    endpoints = ['/users', '/products', '/orders', '/analytics', '/health']
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    
    endpoint = random.choice(endpoints)
    method = random.choice(methods)
    
    # Simulate API call
    response_time = random.uniform(50, 2000)  # ms
    status_code = random.choices([200, 201, 400, 404, 500], weights=[70, 10, 10, 5, 5])[0]
    
    time.sleep(response_time / 1000)  # Convert to seconds
    
    result = {
        'endpoint': endpoint,
        'method': method,
        'response_time_ms': response_time,
        'status_code': status_code,
        'success': status_code < 400
    }
    context['task_instance'].xcom_push(key='api_test_result', value=result)
    return result

def load_test_api(**context):
    """Perform load testing on API"""
    target_time = random.uniform(60, 300)
    start_time = time.time()
    requests_made = 0
    
    while time.time() - start_time < target_time:
        time.sleep(random.uniform(0.01, 0.1))  # Simulate request interval
        requests_made += 1
    
    return {
        'total_requests': requests_made,
        'duration_seconds': time.time() - start_time,
        'requests_per_second': requests_made / (time.time() - start_time)
    }

default_args = {
    'owner': 'qa_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'api_testing_dag',
    default_args=default_args,
    description='API endpoint testing and load testing',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=3,
    concurrency=64,
    tags=['api_testing', 'qa', 'load_testing'],
)

# API test tasks
api_tests = []
for i in range(1, 16):
    task = PythonOperator(
        task_id=f'api_test_{i}',
        python_callable=test_api_endpoint,
        dag=dag,
    )
    api_tests.append(task)

# Load testing tasks
load_tests = []
for i in range(1, 4):
    task = PythonOperator(
        task_id=f'load_test_{i}',
        python_callable=load_test_api,
        dag=dag,
    )
    load_tests.append(task)

# Sequential API tests followed by load tests
for i in range(len(api_tests) - 1):
    if i % 4 == 0:
        api_tests[i] >> api_tests[i + 1]

api_tests[0] >> load_tests[0]
api_tests[5] >> load_tests[1]
