from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import time
import math
import json
import hashlib

def data_processing_task():
    duration = random.randint(5, 45)
    end_time = time.time() + duration
    data = [random.randint(1, 1000) for _ in range(10000)]
    while time.time() < end_time:
        filtered_data = [x for x in data if x % 2 == 0]
        sorted_data = sorted(filtered_data, reverse=True)
        result = sum(sorted_data[:100])
    return result

def network_simulation_task():
    time.sleep(random.randint(2, 30))
    latency_values = [random.uniform(0.1, 2.0) for _ in range(1000)]
    avg_latency = sum(latency_values) / len(latency_values)
    return avg_latency

def hash_computation_task():
    duration = random.randint(10, 90)
    end_time = time.time() + duration
    result_hash = ""
    while time.time() < end_time:
        data = str(random.randint(1, 1000000))
        result_hash = hashlib.sha256(data.encode()).hexdigest()
    return result_hash

def matrix_multiplication_task():
    duration = random.randint(15, 120)
    end_time = time.time() + duration
    while time.time() < end_time:
        matrix_a = [[random.randint(1, 10) for _ in range(50)] for _ in range(50)]
        matrix_b = [[random.randint(1, 10) for _ in range(50)] for _ in range(50)]
        result = [[sum(a*b for a,b in zip(row_a, col_b)) for col_b in zip(*matrix_b)] for row_a in matrix_a]
    return len(result)

def statistical_analysis_task():
    duration = random.randint(20, 100)
    end_time = time.time() + duration
    while time.time() < end_time:
        dataset = [random.gauss(50, 15) for _ in range(5000)]
        mean_val = sum(dataset) / len(dataset)
        variance = sum((x - mean_val) ** 2 for x in dataset) / len(dataset)
        std_dev = math.sqrt(variance)
    return std_dev

def file_processing_simulation():
    time.sleep(random.randint(3, 25))
    file_sizes = [random.randint(1024, 1048576) for _ in range(100)]
    total_size = sum(file_sizes)
    avg_size = total_size / len(file_sizes)
    return avg_size

def api_call_simulation():
    response_times = []
    for _ in range(random.randint(10, 50)):
        time.sleep(random.uniform(0.1, 2.0))
        response_times.append(random.uniform(100, 2000))
    return sum(response_times) / len(response_times)

def database_query_simulation():
    duration = random.randint(8, 60)
    end_time = time.time() + duration
    records_processed = 0
    while time.time() < end_time:
        query_result = [{"id": i, "value": random.randint(1, 1000)} for i in range(1000)]
        records_processed += len(query_result)
    return records_processed

def machine_learning_simulation():
    duration = random.randint(30, 180)
    end_time = time.time() + duration
    while time.time() < end_time:
        training_data = [[random.uniform(-1, 1) for _ in range(10)] for _ in range(1000)]
        weights = [random.uniform(-0.5, 0.5) for _ in range(10)]
        for epoch in range(100):
            for data_point in training_data:
                prediction = sum(w * x for w, x in zip(weights, data_point))
                error = random.uniform(-0.1, 0.1)
    return sum(weights)

def intensive_computation_task():
    duration = random.randint(600, 660)
    end_time = time.time() + duration
    result = 0
    while time.time() < end_time:
        for i in range(1000):
            result += math.factorial(random.randint(1, 20)) % 1000000
            result += sum(math.sin(x) for x in range(1000))
            result += sum(math.sqrt(x) for x in range(1, 1000))
    return result

def get_task_function(task_id):
    task_num = int(task_id.split('_')[1])
    
    if task_num <= 20:
        return data_processing_task
    elif task_num <= 35:
        return network_simulation_task
    elif task_num <= 50:
        return hash_computation_task
    elif task_num <= 65:
        return matrix_multiplication_task
    elif task_num <= 75:
        return statistical_analysis_task
    elif task_num <= 85:
        return file_processing_simulation
    elif task_num <= 92:
        return api_call_simulation
    elif task_num <= 97:
        return database_query_simulation
    elif task_num <= 99:
        return machine_learning_simulation
    else:
        return intensive_computation_task

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
    'dag_100_tasks_tree_dependencies',
    default_args=default_args,
    description='DAG with 100 tasks in tree structure',
    schedule_interval=None,
    catchup=False,
)

tasks = {}

for i in range(1, 101):
    task_func = get_task_function(f'task_{i}')
    
    tasks[f'task_{i}'] = PythonOperator(
        task_id=f'task_{i}',
        python_callable=task_func,
        dag=dag,
    )

tasks['task_2'] >> tasks['task_1']
tasks['task_3'] >> tasks['task_1']
tasks['task_4'] >> tasks['task_2']
tasks['task_5'] >> tasks['task_2']
tasks['task_6'] >> tasks['task_3']
tasks['task_7'] >> tasks['task_3']
tasks['task_8'] >> tasks['task_4']
tasks['task_9'] >> tasks['task_4']
tasks['task_10'] >> tasks['task_5']
tasks['task_11'] >> tasks['task_5']
tasks['task_12'] >> tasks['task_6']
tasks['task_13'] >> tasks['task_6']
tasks['task_14'] >> tasks['task_7']
tasks['task_15'] >> tasks['task_7']
tasks['task_16'] >> tasks['task_8']
tasks['task_17'] >> tasks['task_8']
tasks['task_18'] >> tasks['task_9']
tasks['task_19'] >> tasks['task_9']
tasks['task_20'] >> tasks['task_10']
tasks['task_21'] >> tasks['task_10']
tasks['task_22'] >> tasks['task_11']
tasks['task_23'] >> tasks['task_11']
tasks['task_24'] >> tasks['task_12']
tasks['task_25'] >> tasks['task_12']
tasks['task_26'] >> tasks['task_13']
tasks['task_27'] >> tasks['task_13']
tasks['task_28'] >> tasks['task_14']
tasks['task_29'] >> tasks['task_14']
tasks['task_30'] >> tasks['task_15']
tasks['task_31'] >> tasks['task_15']
tasks['task_32'] >> tasks['task_16']
tasks['task_33'] >> tasks['task_17']
tasks['task_34'] >> tasks['task_18']
tasks['task_35'] >> tasks['task_19']
tasks['task_36'] >> tasks['task_20']
tasks['task_37'] >> tasks['task_21']
tasks['task_38'] >> tasks['task_22']
tasks['task_39'] >> tasks['task_23']
tasks['task_40'] >> tasks['task_24']
tasks['task_41'] >> tasks['task_25']
tasks['task_42'] >> tasks['task_26']
tasks['task_43'] >> tasks['task_27']
tasks['task_44'] >> tasks['task_28']
tasks['task_45'] >> tasks['task_29']
tasks['task_46'] >> tasks['task_30']
tasks['task_47'] >> tasks['task_31']
tasks['task_48'] >> tasks['task_32']
tasks['task_49'] >> tasks['task_33']
tasks['task_50'] >> tasks['task_34']
tasks['task_51'] >> tasks['task_35']
tasks['task_52'] >> tasks['task_36']
tasks['task_53'] >> tasks['task_37']
tasks['task_54'] >> tasks['task_38']
tasks['task_55'] >> tasks['task_39']
tasks['task_56'] >> tasks['task_40']
tasks['task_57'] >> tasks['task_41']
tasks['task_58'] >> tasks['task_42']
tasks['task_59'] >> tasks['task_43']
tasks['task_60'] >> tasks['task_44']
tasks['task_61'] >> tasks['task_45']
tasks['task_62'] >> tasks['task_46']
tasks['task_63'] >> tasks['task_47']
tasks['task_64'] >> tasks['task_48']
tasks['task_65'] >> tasks['task_49']
tasks['task_66'] >> tasks['task_50']
tasks['task_67'] >> tasks['task_51']
tasks['task_68'] >> tasks['task_52']
tasks['task_69'] >> tasks['task_53']
tasks['task_70'] >> tasks['task_54']
tasks['task_71'] >> tasks['task_55']
tasks['task_72'] >> tasks['task_56']
tasks['task_73'] >> tasks['task_57']
tasks['task_74'] >> tasks['task_58']
tasks['task_75'] >> tasks['task_59']
tasks['task_76'] >> tasks['task_60']
tasks['task_77'] >> tasks['task_61']
tasks['task_78'] >> tasks['task_62']
tasks['task_79'] >> tasks['task_63']
tasks['task_80'] >> tasks['task_64']
tasks['task_81'] >> tasks['task_65']
tasks['task_82'] >> tasks['task_66']
tasks['task_83'] >> tasks['task_67']
tasks['task_84'] >> tasks['task_68']
tasks['task_85'] >> tasks['task_69']
tasks['task_86'] >> tasks['task_70']
tasks['task_87'] >> tasks['task_71']
tasks['task_88'] >> tasks['task_72']
tasks['task_89'] >> tasks['task_73']
tasks['task_90'] >> tasks['task_74']
tasks['task_91'] >> tasks['task_75']
tasks['task_92'] >> tasks['task_76']
tasks['task_93'] >> tasks['task_77']
tasks['task_94'] >> tasks['task_78']
tasks['task_95'] >> tasks['task_79']
tasks['task_96'] >> tasks['task_80']
tasks['task_97'] >> tasks['task_81']
tasks['task_98'] >> tasks['task_82']
tasks['task_99'] >> tasks['task_83']
tasks['task_100'] >> tasks['task_84']
