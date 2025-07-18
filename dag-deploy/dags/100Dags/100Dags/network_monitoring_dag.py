"""
Network Monitoring DAG - Network connectivity and performance checks
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def ping_host(**context):
    """Simulate network ping operations"""
    hosts = ['server1', 'server2', 'database', 'api_gateway', 'cache_server']
    host = random.choice(hosts)
    
    # Simulate ping with random response times
    response_time = random.uniform(1, 100)  # ms
    packet_loss = random.uniform(0, 5)  # percentage
    
    time.sleep(random.uniform(5, 60))
    
    result = {
        'host': host,
        'response_time_ms': response_time,
        'packet_loss_percent': packet_loss,
        'status': 'up' if packet_loss < 3 else 'degraded'
    }
    context['task_instance'].xcom_push(key='ping_result', value=result)
    return result

def check_bandwidth(**context):
    """Simulate bandwidth testing"""
    time.sleep(random.uniform(30, 180))
    
    download_speed = random.uniform(50, 1000)  # Mbps
    upload_speed = random.uniform(10, 500)    # Mbps
    
    return {
        'download_mbps': download_speed,
        'upload_mbps': upload_speed,
        'bandwidth_test_complete': True
    }

default_args = {
    'owner': 'network_ops',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'network_monitoring_dag',
    default_args=default_args,
    description='Network monitoring and performance testing',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    max_active_runs=5,
    concurrency=32,
    tags=['network', 'monitoring', 'infrastructure'],
)

# Network monitoring tasks
ping_tasks = []
for i in range(1, 12):
    task = PythonOperator(
        task_id=f'ping_check_{i}',
        python_callable=ping_host,
        dag=dag,
    )
    ping_tasks.append(task)

bandwidth_task = PythonOperator(
    task_id='bandwidth_test',
    python_callable=check_bandwidth,
    dag=dag,
)

# Generate network report
report_task = BashOperator(
    task_id='generate_network_report',
    bash_command='echo "Generating network performance report" && sleep 20',
    dag=dag,
)

# Dependencies - parallel pings then bandwidth test
for task in ping_tasks[:5]:
    task >> bandwidth_task

bandwidth_task >> report_task
