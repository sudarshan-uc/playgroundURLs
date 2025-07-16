from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import time
import math
import hashlib
import string

def cpu_intensive_calculation():
    duration = random.randint(8, 75)
    end_time = time.time() + duration
    result = 0
    while time.time() < end_time:
        for i in range(1000):
            result += math.pow(random.randint(1, 100), 2.5)
            result += math.sqrt(random.randint(1, 1000000))
    return result

def memory_allocation_task():
    duration = random.randint(5, 60)
    end_time = time.time() + duration
    memory_blocks = []
    while time.time() < end_time:
        block_size = random.randint(1000, 100000)
        memory_blocks.append([random.randint(0, 255) for _ in range(block_size)])
        if len(memory_blocks) > 10:
            memory_blocks.pop(0)
    return len(memory_blocks)

def string_manipulation_task():
    time.sleep(random.randint(2, 35))
    text = ''.join(random.choices(string.ascii_letters + string.digits, k=100000))
    reversed_text = text[::-1]
    uppercase_text = text.upper()
    word_count = len(text.split())
    return word_count

def recursive_calculation():
    def fibonacci(n):
        if n <= 1:
            return n
        return fibonacci(n-1) + fibonacci(n-2)
    
    duration = random.randint(10, 90)
    end_time = time.time() + duration
    results = []
    while time.time() < end_time:
        n = random.randint(1, 25)
        results.append(fibonacci(n))
    return sum(results)

def network_delay_simulation():
    total_delay = 0
    for _ in range(random.randint(5, 30)):
        delay = random.uniform(0.1, 4.0)
        time.sleep(delay)
        total_delay += delay
    return total_delay

def data_transformation_task():
    duration = random.randint(12, 85)
    end_time = time.time() + duration
    while time.time() < end_time:
        raw_data = [{"id": i, "value": random.randint(1, 1000), "category": random.choice(['A', 'B', 'C'])} for i in range(5000)]
        filtered_data = [item for item in raw_data if item["value"] > 500]
        grouped_data = {}
        for item in filtered_data:
            category = item["category"]
            if category not in grouped_data:
                grouped_data[category] = []
            grouped_data[category].append(item["value"])
    return len(grouped_data)

def encryption_decryption_task():
    duration = random.randint(15, 100)
    end_time = time.time() + duration
    encrypted_count = 0
    while time.time() < end_time:
        plaintext = ''.join(random.choices(string.ascii_letters, k=1000))
        encrypted = hashlib.sha256(plaintext.encode()).hexdigest()
        decrypted_length = len(encrypted)
        encrypted_count += 1
    return encrypted_count

def prime_number_generation():
    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(math.sqrt(n)) + 1):
            if n % i == 0:
                return False
        return True
    
    duration = random.randint(20, 120)
    end_time = time.time() + duration
    primes = []
    num = 2
    while time.time() < end_time:
        if is_prime(num):
            primes.append(num)
        num += 1
    return len(primes)

def monte_carlo_simulation():
    duration = random.randint(25, 150)
    end_time = time.time() + duration
    inside_circle = 0
    total_points = 0
    while time.time() < end_time:
        for _ in range(10000):
            x = random.uniform(-1, 1)
            y = random.uniform(-1, 1)
            if x*x + y*y <= 1:
                inside_circle += 1
            total_points += 1
    pi_estimate = 4 * inside_circle / total_points if total_points > 0 else 0
    return pi_estimate

def massive_computation_task():
    duration = random.randint(600, 660)
    end_time = time.time() + duration
    result = 0
    while time.time() < end_time:
        for i in range(200):
            result += math.factorial(random.randint(1, 20)) % 1000000
            result += sum(math.sin(x * math.pi / 180) for x in range(720))
            result += sum(math.log(x) for x in range(1, 2000))
            matrix_a = [[random.randint(1, 10) for _ in range(20)] for _ in range(20)]
            matrix_b = [[random.randint(1, 10) for _ in range(20)] for _ in range(20)]
            matrix_result = [[sum(a*b for a,b in zip(row_a, col_b)) for col_b in zip(*matrix_b)] for row_a in matrix_a]
            result += len(matrix_result)
    return result

def get_task_function(task_id):
    task_num = int(task_id.split('_')[1])
    
    if task_num <= 80:
        return cpu_intensive_calculation
    elif task_num <= 150:
        return memory_allocation_task
    elif task_num <= 220:
        return string_manipulation_task
    elif task_num <= 280:
        return recursive_calculation
    elif task_num <= 330:
        return network_delay_simulation
    elif task_num <= 380:
        return data_transformation_task
    elif task_num <= 430:
        return encryption_decryption_task
    elif task_num <= 470:
        return prime_number_generation
    elif task_num <= 495:
        return monte_carlo_simulation
    else:
        return massive_computation_task

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
    'dag_500_tasks_partial_parallel_200',
    default_args=default_args,
    description='DAG with 500 tasks, 200 parallel execution with dependencies',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=200,
)

tasks = {}

for i in range(1, 501):
    task_func = get_task_function(f'task_{i}')
    
    tasks[f'task_{i}'] = PythonOperator(
        task_id=f'task_{i}',
        python_callable=task_func,
        dag=dag,
    )

tasks['task_2'] >> tasks['task_1']
tasks['task_3'] >> tasks['task_1']
tasks['task_5'] >> tasks['task_4']
tasks['task_6'] >> tasks['task_4']
tasks['task_8'] >> tasks['task_7']
tasks['task_9'] >> tasks['task_7']
tasks['task_11'] >> tasks['task_10']
tasks['task_12'] >> tasks['task_10']
tasks['task_14'] >> tasks['task_13']
tasks['task_15'] >> tasks['task_13']
tasks['task_17'] >> tasks['task_16']
tasks['task_18'] >> tasks['task_16']
tasks['task_20'] >> tasks['task_19']
tasks['task_21'] >> tasks['task_19']
[tasks['task_22'], tasks['task_23']] >> tasks['task_2']
[tasks['task_24'], tasks['task_25']] >> tasks['task_3']
[tasks['task_26'], tasks['task_27']] >> tasks['task_5']
[tasks['task_28'], tasks['task_29']] >> tasks['task_6']
[tasks['task_30'], tasks['task_31']] >> tasks['task_8']
[tasks['task_32'], tasks['task_33']] >> tasks['task_9']
[tasks['task_34'], tasks['task_35']] >> tasks['task_11']
[tasks['task_36'], tasks['task_37']] >> tasks['task_12']
[tasks['task_38'], tasks['task_39']] >> tasks['task_14']
[tasks['task_40'], tasks['task_41']] >> tasks['task_15']
tasks['task_50'] >> tasks['task_22']
tasks['task_60'] >> tasks['task_24']
tasks['task_70'] >> tasks['task_26']
tasks['task_80'] >> tasks['task_28']
tasks['task_90'] >> tasks['task_30']
tasks['task_100'] >> tasks['task_32']
tasks['task_110'] >> tasks['task_34']
tasks['task_120'] >> tasks['task_36']
tasks['task_130'] >> tasks['task_38']
tasks['task_140'] >> tasks['task_40']
