"""
Dynamic DAG Generator for Airflow Benchmarking

This module generates multiple DAGs with varying configurations:
- 500, 1000, or 1500 DAGs can be generated
- Each DAG has 2-50 tasks randomly
- Tasks have 1-4 sequential dependencies
- Tasks perform random mathematical operations taking 5 seconds to 5 minutes
- DAGs have varied settings for realistic benchmarking
"""

import random
import math
import time
from datetime import timedelta
from typing import Dict, List, Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Configuration for DAG generation
DAG_CONFIGS = {
    'num_dags': 5,  # Reduced for better performance - can be increased gradually
    'min_tasks_per_dag': 2,
    'max_tasks_per_dag': 10,  # Reduced for better performance
    'min_execution_time': 5,  # seconds
    'max_execution_time': 60,  # Reduced to 1 minute for faster testing
    'max_sequential_depth': 4,
}

# Fixed seed for consistent DAG generation
RANDOM_SEED = 42

# Varied schedule intervals for different DAGs
SCHEDULE_INTERVALS = [
    '@once',
    '@daily',
    '@hourly',
    '@weekly',
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=30),
    timedelta(hours=2),
    timedelta(hours=6),
    timedelta(hours=12),
    timedelta(days=2),
    timedelta(days=3),
    None,  # Manual trigger only
]

# Different catchup settings
CATCHUP_OPTIONS = [True, False]

# Different max_active_runs settings
MAX_ACTIVE_RUNS_OPTIONS = [1, 2, 3, 5, 10, 16]

# Different concurrency settings
CONCURRENCY_OPTIONS = [16, 32, 64, 128, 256]

# Different default_args variations
DEFAULT_ARGS_VARIATIONS = [
    {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    {
        'owner': 'benchmark_user',
        'depends_on_past': True,
        'start_date': days_ago(2),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    {
        'owner': 'test_user',
        'depends_on_past': False,
        'start_date': days_ago(0),
        'email_on_failure': False,
        'email_on_retry': True,
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
    {
        'owner': 'data_engineer',
        'depends_on_past': False,
        'start_date': days_ago(3),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=15),
    },
]


def generate_complex_math_operation(dag_index: int = 0) -> Tuple[str, float]:
    """
    Generate a random complex mathematical operation with variable execution time.
    Returns operation description and target execution time.
    Uses deterministic random generation based on dag_index for consistency.
    """
    operations = [
        'fibonacci_calculation',
        'prime_factorization',
        'matrix_multiplication',
        'monte_carlo_pi',
        'sorting_algorithm',
        'hash_computation',
        'factorial_calculation',
        'power_calculation',
        'trigonometric_series',
        'polynomial_evaluation'
    ]
    
    # Set seed based on dag_index for consistent generation
    local_random = random.Random(RANDOM_SEED + dag_index + len(operations))
    
    operation = local_random.choice(operations)
    target_time = local_random.uniform(
        DAG_CONFIGS['min_execution_time'], 
        DAG_CONFIGS['max_execution_time']
    )
    
    return operation, target_time


def perform_math_operation(operation_type: str, target_time: float, **context) -> Dict[str, Any]:
    """
    Perform a mathematical operation that takes approximately the target time.
    """
    start_time = time.time()
    result = {}
    
    if operation_type == 'fibonacci_calculation':
        # Calculate Fibonacci numbers until target time is reached
        a, b = 0, 1
        count = 0
        while time.time() - start_time < target_time:
            a, b = b, a + b
            count += 1
            if count % 1000 == 0:  # Prevent too tight loop
                time.sleep(0.001)
        result = {'fibonacci_count': count, 'last_fib': b}
        
    elif operation_type == 'prime_factorization':
        # Factor random numbers until target time is reached
        factors_found = []
        while time.time() - start_time < target_time:
            num = random.randint(1000, 100000)
            factors = []
            d = 2
            while d * d <= num:
                while num % d == 0:
                    factors.append(d)
                    num //= d
                d += 1
            if num > 1:
                factors.append(num)
            factors_found.append(factors)
            time.sleep(0.01)  # Small delay to control execution time
        result = {'numbers_factored': len(factors_found)}
        
    elif operation_type == 'matrix_multiplication':
        # Multiply matrices until target time is reached
        multiplications = 0
        while time.time() - start_time < target_time:
            size = random.randint(10, 50)
            matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
            matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]
            
            # Matrix multiplication
            result_matrix = [[0.0 for _ in range(size)] for _ in range(size)]
            for i in range(size):
                for j in range(size):
                    for k in range(size):
                        result_matrix[i][j] += matrix_a[i][k] * matrix_b[k][j]
            multiplications += 1
        result = {'matrix_multiplications': multiplications}
        
    elif operation_type == 'monte_carlo_pi':
        # Estimate Pi using Monte Carlo method
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
        
    elif operation_type == 'sorting_algorithm':
        # Sort random arrays until target time is reached
        sorts_completed = 0
        while time.time() - start_time < target_time:
            size = random.randint(100, 1000)
            arr = [random.randint(1, 1000) for _ in range(size)]
            # Bubble sort (intentionally inefficient for time consumption)
            n = len(arr)
            for i in range(n):
                for j in range(0, n-i-1):
                    if arr[j] > arr[j+1]:
                        arr[j], arr[j+1] = arr[j+1], arr[j]
            sorts_completed += 1
        result = {'arrays_sorted': sorts_completed}
        
    elif operation_type == 'hash_computation':
        # Compute hashes until target time is reached
        hashes_computed = 0
        while time.time() - start_time < target_time:
            data = str(random.randint(1, 1000000)).encode()
            _ = hash(data)  # Perform hash computation
            hashes_computed += 1
            if hashes_computed % 1000 == 0:
                time.sleep(0.001)
        result = {'hashes_computed': hashes_computed}
        
    elif operation_type == 'factorial_calculation':
        # Calculate factorials until target time is reached
        factorials_calculated = 0
        while time.time() - start_time < target_time:
            n = random.randint(1, 100)
            _ = math.factorial(n)  # Perform factorial calculation
            factorials_calculated += 1
            time.sleep(0.01)
        result = {'factorials_calculated': factorials_calculated}
        
    elif operation_type == 'power_calculation':
        # Calculate powers until target time is reached
        powers_calculated = 0
        while time.time() - start_time < target_time:
            base = random.uniform(1, 10)
            exponent = random.uniform(1, 5)
            _ = base ** exponent  # Perform power calculation
            powers_calculated += 1
            time.sleep(0.01)
        result = {'powers_calculated': powers_calculated}
        
    elif operation_type == 'trigonometric_series':
        # Calculate trigonometric series until target time is reached
        calculations = 0
        while time.time() - start_time < target_time:
            x = random.uniform(0, 2 * math.pi)
            _ = sum(math.sin(x * i) for i in range(1, 100))  # Sin calculation
            _ = sum(math.cos(x * i) for i in range(1, 100))  # Cos calculation
            calculations += 1
            time.sleep(0.01)
        result = {'trig_calculations': calculations}
        
    else:  # polynomial_evaluation
        # Evaluate polynomials until target time is reached
        evaluations = 0
        while time.time() - start_time < target_time:
            coefficients = [random.uniform(-10, 10) for _ in range(10)]
            x = random.uniform(-5, 5)
            _ = sum(coef * (x ** i) for i, coef in enumerate(coefficients))  # Polynomial evaluation
            evaluations += 1
            time.sleep(0.01)
        result = {'polynomial_evaluations': evaluations}
    
    actual_time = time.time() - start_time
    
    # Create a new dictionary with the additional information
    final_result: Dict[str, Any] = dict(result)
    final_result['operation_type'] = operation_type
    final_result['target_time'] = target_time
    final_result['actual_time'] = actual_time
    final_result['task_id'] = context['task_instance'].task_id
    final_result['dag_id'] = context['task_instance'].dag_id
    
    return final_result


def generate_dag_structure(num_tasks: int, dag_index: int) -> List[Tuple[str, List[str]]]:
    """
    Generate DAG structure with tasks and their dependencies.
    Returns list of (task_id, dependencies) tuples.
    Uses deterministic random generation based on dag_index for consistency.
    """
    # Set seed based on dag_index for consistent generation
    local_random = random.Random(RANDOM_SEED + dag_index)
    
    tasks = []
    task_names = [f"task_{i:03d}" for i in range(num_tasks)]
    
    # First task has no dependencies
    tasks.append((task_names[0], []))
    
    for i in range(1, num_tasks):
        # Determine number of dependencies (1-4 sequential tasks)
        max_deps = min(DAG_CONFIGS['max_sequential_depth'], i)
        num_deps = local_random.randint(1, max_deps)
        
        # Select dependencies from previous tasks
        possible_deps = task_names[:i]
        dependencies = local_random.sample(possible_deps, min(num_deps, len(possible_deps)))
        
        tasks.append((task_names[i], dependencies))
    
    return tasks


def create_dynamic_dag(dag_id: str, dag_config: Dict[str, Any], dag_index: int) -> DAG:
    """
    Create a single dynamic DAG with the given configuration.
    """
    # Set seed based on dag_index for consistent generation
    local_random = random.Random(RANDOM_SEED + dag_index)
    
    # Generate DAG structure
    num_tasks = local_random.randint(
        DAG_CONFIGS['min_tasks_per_dag'], 
        DAG_CONFIGS['max_tasks_per_dag']
    )
    task_structure = generate_dag_structure(num_tasks, dag_index)
    
    # Create DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=dag_config['default_args'],
        description=f'Dynamic benchmark DAG with {num_tasks} tasks',
        schedule_interval=dag_config['schedule_interval'],
        start_date=dag_config['default_args']['start_date'],
        catchup=dag_config['catchup'],
        max_active_runs=dag_config['max_active_runs'],
        concurrency=dag_config['concurrency'],
        tags=['benchmark', 'dynamic', 'generated'],
    )
    
    # Create tasks
    tasks = {}
    for task_id, dependencies in task_structure:
        operation_type, target_time = generate_complex_math_operation(dag_index)
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=perform_math_operation,
            op_kwargs={
                'operation_type': operation_type,
                'target_time': target_time,
            },
            dag=dag,
        )
        
        tasks[task_id] = task
    
    # Set up dependencies
    for task_id, dependencies in task_structure:
        if dependencies:
            for dep in dependencies:
                # Set task dependency
                _ = tasks[dep] >> tasks[task_id]
    
    return dag


# Generate DAGs using proper Airflow dynamic DAG generation
def create_dag_config(dag_index: int) -> Dict[str, Any]:
    """
    Create DAG configuration for a specific DAG index.
    Uses deterministic random generation for consistency.
    """
    local_random = random.Random(RANDOM_SEED + dag_index)
    
    config = {
        'default_args': local_random.choice(DEFAULT_ARGS_VARIATIONS).copy(),
        'schedule_interval': local_random.choice(SCHEDULE_INTERVALS),
        'catchup': local_random.choice(CATCHUP_OPTIONS),
        'max_active_runs': local_random.choice(MAX_ACTIVE_RUNS_OPTIONS),
        'concurrency': local_random.choice(CONCURRENCY_OPTIONS),
    }
    
    # Slight variation in start dates
    start_date_offset = local_random.randint(0, 7)
    config['default_args']['start_date'] = days_ago(start_date_offset)
    
    return config


# Create individual DAG instances using a loop
# This is the recommended approach for dynamic DAG generation in Airflow
for i in range(DAG_CONFIGS['num_dags']):
    dag_id = f"dynamic_benchmark_dag_{i:04d}"
    
    try:
        # Create DAG configuration
        dag_config = create_dag_config(i)
        
        # Create the DAG
        dag = create_dynamic_dag(dag_id, dag_config, i)
        
        # Add to globals with the DAG ID as the variable name
        # This is the proper way to expose DAGs to Airflow
        globals()[dag_id] = dag
        
    except Exception as e:
        print(f"Error creating DAG {dag_id}: {str(e)}")
        continue

print(f"Generated {DAG_CONFIGS['num_dags']} dynamic DAGs successfully")
