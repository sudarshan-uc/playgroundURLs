"""
Monte Carlo Simulation DAG - Statistical computation
"""
import random
import time
import math
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def monte_carlo_pi_simulation(**context):
    """Estimate Pi using Monte Carlo method"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    inside_circle = 0
    total_points = 0
    
    while time.time() - start_time < target_time:
        x, y = random.random(), random.random()
        if x*x + y*y <= 1:
            inside_circle += 1
        total_points += 1
        if total_points % 10000 == 0:
            time.sleep(0.001)
    
    pi_estimate = 4 * inside_circle / total_points if total_points > 0 else 0
    result = {'pi_estimate': pi_estimate, 'total_points': total_points}
    context['task_instance'].xcom_push(key='simulation_result', value=result)
    return result

default_args = {
    'owner': 'statistics_team',
    'depends_on_past': False,
    'start_date': days_ago(3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'monte_carlo_simulation_dag',
    default_args=default_args,
    description='Monte Carlo Pi estimation simulation',
    schedule_interval=timedelta(hours=8),
    catchup=True,
    max_active_runs=2,
    concurrency=16,
    tags=['statistics', 'monte_carlo', 'simulation'],
)

# Create simulation tasks
simulation_tasks = []
for i in range(1, 15):
    task = PythonOperator(
        task_id=f'monte_carlo_sim_{i}',
        python_callable=monte_carlo_pi_simulation,
        dag=dag,
    )
    simulation_tasks.append(task)

# Create dependencies with some sequential chains
simulation_tasks[0] >> simulation_tasks[1] >> simulation_tasks[2]
simulation_tasks[3] >> simulation_tasks[4]
simulation_tasks[5] >> simulation_tasks[6] >> simulation_tasks[7] >> simulation_tasks[8]
