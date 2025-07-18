"""
Cryptographic Hash DAG - Hash computation benchmark
"""
import random
import time
import hashlib
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def hash_computation_task(**context):
    """Compute various hash functions"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    hashes_computed = 0
    
    hash_functions = [hashlib.md5, hashlib.sha1, hashlib.sha256, hashlib.sha512]
    
    while time.time() - start_time < target_time:
        data = str(random.randint(1, 1000000)).encode()
        hash_func = random.choice(hash_functions)
        hash_obj = hash_func(data)
        hash_digest = hash_obj.hexdigest()
        hashes_computed += 1
        if hashes_computed % 1000 == 0:
            time.sleep(0.001)
    
    result = {'hashes_computed': hashes_computed}
    context['task_instance'].xcom_push(key='hash_result', value=result)
    return result

default_args = {
    'owner': 'crypto_team',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cryptographic_hash_dag',
    default_args=default_args,
    description='Cryptographic hash computation benchmark',
    schedule_interval='@weekly',
    catchup=True,
    max_active_runs=1,
    concurrency=16,
    tags=['cryptography', 'hash', 'security'],
)

# Hash computation tasks
hash_tasks = []
for i in range(1, 19):
    task = PythonOperator(
        task_id=f'hash_compute_{i}',
        python_callable=hash_computation_task,
        dag=dag,
    )
    hash_tasks.append(task)

# Create some sequential chains
hash_tasks[0] >> hash_tasks[1] >> hash_tasks[2] >> hash_tasks[3]
hash_tasks[4] >> hash_tasks[5]
hash_tasks[6] >> hash_tasks[7] >> hash_tasks[8]
