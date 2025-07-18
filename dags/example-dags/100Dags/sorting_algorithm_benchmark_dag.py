"""
Sorting Algorithm Benchmark DAG - Array sorting operations
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def bubble_sort_task(**context):
    """Perform bubble sort on random arrays"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    sorts_completed = 0
    
    while time.time() - start_time < target_time:
        size = random.randint(100, 1000)
        arr = [random.randint(1, 1000) for _ in range(size)]
        n = len(arr)
        for i in range(n):
            for j in range(0, n-i-1):
                if arr[j] > arr[j+1]:
                    arr[j], arr[j+1] = arr[j+1], arr[j]
        sorts_completed += 1
    
    result = {'algorithm': 'bubble_sort', 'arrays_sorted': sorts_completed}
    context['task_instance'].xcom_push(key='sort_result', value=result)
    return result

def quick_sort_task(**context):
    """Perform quick sort on random arrays"""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    sorts_completed = 0
    
    def quicksort(arr):
        if len(arr) <= 1:
            return arr
        pivot = arr[len(arr) // 2]
        left = [x for x in arr if x < pivot]
        middle = [x for x in arr if x == pivot]
        right = [x for x in arr if x > pivot]
        return quicksort(left) + middle + quicksort(right)
    
    while time.time() - start_time < target_time:
        size = random.randint(100, 2000)
        arr = [random.randint(1, 1000) for _ in range(size)]
        sorted_arr = quicksort(arr)
        sorts_completed += 1
        if sorts_completed % 10 == 0:
            time.sleep(0.01)
    
    result = {'algorithm': 'quick_sort', 'arrays_sorted': sorts_completed}
    context['task_instance'].xcom_push(key='sort_result', value=result)
    return result

default_args = {
    'owner': 'algorithms_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=8),
}

dag = DAG(
    'sorting_algorithm_benchmark_dag',
    default_args=default_args,
    description='Sorting algorithm performance benchmark',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    max_active_runs=1,
    concurrency=32,
    tags=['algorithms', 'sorting', 'benchmark'],
)

# Sorting tasks
bubble_tasks = []
quick_tasks = []

for i in range(1, 7):
    bubble_task = PythonOperator(
        task_id=f'bubble_sort_{i}',
        python_callable=bubble_sort_task,
        dag=dag,
    )
    bubble_tasks.append(bubble_task)

for i in range(1, 9):
    quick_task = PythonOperator(
        task_id=f'quick_sort_{i}',
        python_callable=quick_sort_task,
        dag=dag,
    )
    quick_tasks.append(quick_task)

# Set sequential dependencies within each algorithm type
for i in range(len(bubble_tasks) - 1):
    bubble_tasks[i] >> bubble_tasks[i + 1]

bubble_tasks[0] >> quick_tasks[0]
quick_tasks[0] >> quick_tasks[1] >> quick_tasks[2]
