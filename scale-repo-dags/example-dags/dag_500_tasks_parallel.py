from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import time
import math

def task_operation(task_type, duration_range):
    if task_type == 'sleep':
        sleep_time = random.randint(*duration_range)
        time.sleep(sleep_time)
    elif task_type == 'math':
        duration = random.randint(*duration_range)
        end_time = time.time() + duration
        result = 0
        while time.time() < end_time:
            result += math.sqrt(random.randint(1, 1000000)) * math.sin(random.uniform(0, 10))
        return result
    elif task_type == 'long_math':
        duration = random.randint(600, 650)
        end_time = time.time() + duration
        result = 0
        while time.time() < end_time:
            for i in range(1000):
                result += math.factorial(random.randint(1, 20)) % 1000000
        return result

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_500_tasks_parallel_200',
    default_args=default_args,
    description='DAG with 500 tasks, 200 parallel execution',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=200,
)

tasks = {}

for i in range(1, 501):
    if i <= 350:
        task_type = 'math'
        duration_range = (1, 120)
    elif i <= 450:
        task_type = 'sleep'
        duration_range = (1, 180)
    else:
        task_type = 'long_math'
        duration_range = (600, 650)
    
    tasks[f'task_{i}'] = PythonOperator(
        task_id=f'task_{i}',
        python_callable=task_operation,
        op_args=[task_type, duration_range],
        dag=dag,
    )

tasks['task_2'] >> tasks['task_1']
tasks['task_3'] >> tasks['task_1']
tasks['task_4'] >> tasks['task_1']
tasks['task_8'] >> tasks['task_5']
tasks['task_9'] >> tasks['task_6']
tasks['task_10'] >> tasks['task_7']
tasks['task_15'] >> tasks['task_11']
tasks['task_16'] >> tasks['task_12']
tasks['task_17'] >> tasks['task_13']
tasks['task_18'] >> tasks['task_14']
tasks['task_25'] >> tasks['task_19']
tasks['task_26'] >> tasks['task_20']
tasks['task_27'] >> tasks['task_21']
tasks['task_28'] >> tasks['task_22']
tasks['task_29'] >> tasks['task_23']
tasks['task_30'] >> tasks['task_24']
tasks['task_40'] >> tasks['task_31']
tasks['task_41'] >> tasks['task_32']
tasks['task_42'] >> tasks['task_33']
tasks['task_43'] >> tasks['task_34']
tasks['task_44'] >> tasks['task_35']
tasks['task_45'] >> tasks['task_36']
tasks['task_46'] >> tasks['task_37']
tasks['task_47'] >> tasks['task_38']
tasks['task_48'] >> tasks['task_39']
tasks['task_60'] >> tasks['task_49']
tasks['task_61'] >> tasks['task_50']
tasks['task_62'] >> tasks['task_51']
tasks['task_63'] >> tasks['task_52']
tasks['task_64'] >> tasks['task_53']
tasks['task_65'] >> tasks['task_54']
tasks['task_66'] >> tasks['task_55']
tasks['task_67'] >> tasks['task_56']
tasks['task_68'] >> tasks['task_57']
tasks['task_69'] >> tasks['task_58']
tasks['task_70'] >> tasks['task_59']
