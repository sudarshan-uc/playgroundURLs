from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import time
import math
import hashlib
import string

def text_processing_task():
    duration = random.randint(5, 45)
    end_time = time.time() + duration
    text_data = ''.join(random.choices(string.ascii_letters + string.digits, k=50000))
    while time.time() < end_time:
        words = text_data.split()
        word_count = len(words)
        unique_chars = len(set(text_data))
    return word_count + unique_chars

def sorting_algorithms_task():
    duration = random.randint(10, 80)
    end_time = time.time() + duration
    while time.time() < end_time:
        data = [random.randint(1, 10000) for _ in range(5000)]
        bubble_sort_steps = 0
        for i in range(len(data)):
            for j in range(len(data) - 1 - i):
                if data[j] > data[j + 1]:
                    data[j], data[j + 1] = data[j + 1], data[j]
                    bubble_sort_steps += 1
    return bubble_sort_steps

def compression_simulation():
    time.sleep(random.randint(3, 30))
    original_size = random.randint(1000000, 10000000)
    compression_ratio = random.uniform(0.3, 0.8)
    compressed_size = int(original_size * compression_ratio)
    return compressed_size

def encryption_task():
    duration = random.randint(8, 60)
    end_time = time.time() + duration
    while time.time() < end_time:
        plaintext = ''.join(random.choices(string.ascii_letters, k=1000))
        encrypted_data = hashlib.sha256(plaintext.encode()).hexdigest()
        decrypted_length = len(encrypted_data)
    return decrypted_length

def image_processing_simulation():
    duration = random.randint(15, 100)
    end_time = time.time() + duration
    while time.time() < end_time:
        image_matrix = [[random.randint(0, 255) for _ in range(100)] for _ in range(100)]
        filtered_matrix = [[max(0, min(255, pixel + random.randint(-50, 50))) for pixel in row] for row in image_matrix]
        histogram = [0] * 256
        for row in filtered_matrix:
            for pixel in row:
                histogram[pixel] += 1
    return sum(histogram)

def web_scraping_simulation():
    pages_scraped = 0
    for _ in range(random.randint(5, 25)):
        time.sleep(random.uniform(0.5, 3.0))
        page_size = random.randint(1024, 102400)
        pages_scraped += 1
    return pages_scraped

def log_analysis_task():
    duration = random.randint(12, 90)
    end_time = time.time() + duration
    log_entries = 0
    while time.time() < end_time:
        log_line = f"{random.choice(['INFO', 'ERROR', 'WARNING'])} - {random.randint(100, 999)}"
        if 'ERROR' in log_line:
            log_entries += 2
        else:
            log_entries += 1
    return log_entries

def graph_algorithms_task():
    duration = random.randint(20, 120)
    end_time = time.time() + duration
    while time.time() < end_time:
        nodes = random.randint(50, 200)
        adjacency_matrix = [[random.randint(0, 1) for _ in range(nodes)] for _ in range(nodes)]
        visited = [False] * nodes
        dfs_count = 0
        for i in range(nodes):
            if not visited[i]:
                stack = [i]
                while stack:
                    node = stack.pop()
                    if not visited[node]:
                        visited[node] = True
                        dfs_count += 1
                        for j in range(nodes):
                            if adjacency_matrix[node][j] and not visited[j]:
                                stack.append(j)
    return dfs_count

def financial_calculation_task():
    duration = random.randint(25, 150)
    end_time = time.time() + duration
    portfolio_value = 0
    while time.time() < end_time:
        stock_prices = [random.uniform(10, 1000) for _ in range(100)]
        weights = [random.uniform(0, 1) for _ in range(100)]
        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]
        portfolio_value = sum(price * weight for price, weight in zip(stock_prices, normalized_weights))
    return portfolio_value

def complex_math_operations():
    duration = random.randint(600, 660)
    end_time = time.time() + duration
    result = 0
    while time.time() < end_time:
        for i in range(500):
            result += math.factorial(random.randint(1, 20)) % 1000000
            result += sum(math.sin(x * math.pi / 180) for x in range(360))
            result += sum(math.log(x) for x in range(1, 1000))
            fourier_component = sum(math.cos(2 * math.pi * k * random.uniform(0, 1)) for k in range(100))
            result += fourier_component
    return result

def get_task_function(task_id):
    task_num = int(task_id.split('_')[1])
    
    if task_num <= 40:
        return text_processing_task
    elif task_num <= 75:
        return sorting_algorithms_task
    elif task_num <= 105:
        return compression_simulation
    elif task_num <= 130:
        return encryption_task
    elif task_num <= 155:
        return image_processing_simulation
    elif task_num <= 170:
        return web_scraping_simulation
    elif task_num <= 185:
        return log_analysis_task
    elif task_num <= 195:
        return graph_algorithms_task
    elif task_num <= 199:
        return financial_calculation_task
    else:
        return complex_math_operations

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
    'dag_200_tasks_parallel_100',
    default_args=default_args,
    description='DAG with 200 tasks, 100 parallel execution',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=100,
)

tasks = {}

for i in range(1, 201):
    task_func = get_task_function(f'task_{i}')
    
    tasks[f'task_{i}'] = PythonOperator(
        task_id=f'task_{i}',
        python_callable=task_func,
        dag=dag,
    )

tasks['task_2'] >> tasks['task_1']
tasks['task_3'] >> tasks['task_1']
tasks['task_6'] >> tasks['task_4']
tasks['task_7'] >> tasks['task_5']
tasks['task_10'] >> tasks['task_8']
tasks['task_11'] >> tasks['task_9']
tasks['task_14'] >> tasks['task_12']
tasks['task_15'] >> tasks['task_13']
tasks['task_18'] >> tasks['task_16']
tasks['task_19'] >> tasks['task_17']
tasks['task_22'] >> tasks['task_20']
tasks['task_23'] >> tasks['task_21']
tasks['task_26'] >> tasks['task_24']
tasks['task_27'] >> tasks['task_25']
tasks['task_30'] >> tasks['task_28']
tasks['task_31'] >> tasks['task_29']
tasks['task_34'] >> tasks['task_32']
tasks['task_35'] >> tasks['task_33']
tasks['task_38'] >> tasks['task_36']
tasks['task_39'] >> tasks['task_37']
tasks['task_42'] >> tasks['task_40']
tasks['task_43'] >> tasks['task_41']
tasks['task_46'] >> tasks['task_44']
tasks['task_47'] >> tasks['task_45']
tasks['task_50'] >> tasks['task_48']
tasks['task_51'] >> tasks['task_49']
