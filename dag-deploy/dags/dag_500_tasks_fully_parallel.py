from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import time
import math
import hashlib
import string

def parallel_search_task():
    duration = random.randint(5, 50)
    end_time = time.time() + duration
    search_results = 0
    while time.time() < end_time:
        dataset = [random.randint(1, 10000) for _ in range(5000)]
        target = random.randint(1, 10000)
        if target in dataset:
            search_results += 1
    return search_results

def concurrent_hash_task():
    duration = random.randint(8, 70)
    end_time = time.time() + duration
    hashes_computed = 0
    while time.time() < end_time:
        data = str(random.randint(1, 1000000))
        hash1 = hashlib.md5(data.encode()).hexdigest()
        hash2 = hashlib.sha1(data.encode()).hexdigest()
        hash3 = hashlib.sha256(data.encode()).hexdigest()
        hashes_computed += 3
    return hashes_computed

def independent_math_task():
    duration = random.randint(10, 85)
    end_time = time.time() + duration
    calculations = 0
    while time.time() < end_time:
        a = random.uniform(1, 100)
        b = random.uniform(1, 100)
        result = math.pow(a, b % 5)
        result += math.sin(a) * math.cos(b)
        result += math.log(a + 1) * math.sqrt(b)
        calculations += 1
    return calculations

def async_file_task():
    time.sleep(random.randint(3, 40))
    file_operations = 0
    for _ in range(random.randint(10, 100)):
        file_size = random.randint(1024, 1048576)
        operation_type = random.choice(['read', 'write', 'append'])
        file_operations += 1
    return file_operations

def parallel_sort_task():
    duration = random.randint(12, 95)
    end_time = time.time() + duration
    arrays_sorted = 0
    while time.time() < end_time:
        array_size = random.randint(1000, 5000)
        data_array = [random.randint(1, 10000) for _ in range(array_size)]
        sorted_array = sorted(data_array)
        arrays_sorted += 1
    return arrays_sorted

def independent_network_task():
    latency_measurements = []
    for _ in range(random.randint(5, 50)):
        latency = random.uniform(0.1, 5.0)
        time.sleep(latency)
        latency_measurements.append(latency)
    return sum(latency_measurements) / len(latency_measurements)

def concurrent_string_task():
    duration = random.randint(7, 60)
    end_time = time.time() + duration
    string_operations = 0
    while time.time() < end_time:
        text = ''.join(random.choices(string.ascii_letters, k=1000))
        text_upper = text.upper()
        text_lower = text.lower()
        text_reversed = text[::-1]
        string_operations += 4
    return string_operations

def parallel_validation_task():
    duration = random.randint(15, 100)
    end_time = time.time() + duration
    validations = 0
    while time.time() < end_time:
        email = f"user{random.randint(1, 1000)}@example.com"
        phone = f"+1{random.randint(1000000000, 9999999999)}"
        credit_card = f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        validations += 3
    return validations

def independent_calculation_task():
    duration = random.randint(20, 120)
    end_time = time.time() + duration
    computations = 0
    while time.time() < end_time:
        matrix_size = random.randint(10, 50)
        matrix = [[random.randint(1, 100) for _ in range(matrix_size)] for _ in range(matrix_size)]
        determinant_approx = sum(matrix[i][i] for i in range(matrix_size))
        trace = sum(matrix[i][i] for i in range(matrix_size))
        computations += 1
    return computations

def massive_parallel_task():
    duration = random.randint(600, 660)
    end_time = time.time() + duration
    operations = 0
    while time.time() < end_time:
        for i in range(100):
            result = math.factorial(random.randint(1, 20)) % 1000000
            result += sum(math.sin(x * math.pi / 180) for x in range(360))
            result += sum(math.log(x) for x in range(1, 1000))
            data_chunk = [random.randint(1, 1000) for _ in range(1000)]
            sorted_chunk = sorted(data_chunk)
            hash_value = hashlib.sha256(str(sorted_chunk).encode()).hexdigest()
            operations += 1
    return operations

def get_task_function(task_id):
    task_num = int(task_id.split('_')[1])
    
    if task_num <= 70:
        return parallel_search_task
    elif task_num <= 130:
        return concurrent_hash_task
    elif task_num <= 190:
        return independent_math_task
    elif task_num <= 240:
        return async_file_task
    elif task_num <= 290:
        return parallel_sort_task
    elif task_num <= 340:
        return independent_network_task
    elif task_num <= 390:
        return concurrent_string_task
    elif task_num <= 440:
        return parallel_validation_task
    elif task_num <= 495:
        return independent_calculation_task
    else:
        return massive_parallel_task

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
    'dag_500_tasks_fully_parallel',
    default_args=default_args,
    description='DAG with 500 fully independent parallel tasks',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=500,
)

tasks = {}

for i in range(1, 501):
    task_func = get_task_function(f'task_{i}')
    
    tasks[f'task_{i}'] = PythonOperator(
        task_id=f'task_{i}',
        python_callable=task_func,
        dag=dag,
    )
