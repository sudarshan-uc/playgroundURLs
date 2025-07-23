"""
100 DAGs Generator for Airflow Benchmarking

This module generates 100 unique DAGs with varying configurations:
- Each DAG has 2-100 tasks randomly
- Tasks have 1-4 sequential dependencies
- Tasks perform various operations: math, XCom, examples, etc.
- Each DAG has unique settings and meaningful names
- Tasks take 5 seconds to 5 minutes to complete
"""

import random
import math
import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


# DAG name templates with operation types
DAG_NAME_TEMPLATES = [
    "fibonacci_sequence_analysis",
    "prime_number_discovery", 
    "matrix_computation_pipeline",
    "monte_carlo_simulation",
    "data_sorting_benchmark",
    "cryptographic_hash_chain",
    "statistical_analysis_workflow",
    "machine_learning_pipeline",
    "data_transformation_etl",
    "scientific_computation",
    "financial_modeling_suite",
    "weather_data_processing",
    "log_analysis_pipeline",
    "image_processing_workflow",
    "text_analytics_engine",
    "sensor_data_aggregation",
    "recommendation_engine",
    "fraud_detection_system",
    "inventory_management",
    "customer_segmentation",
    "sales_forecasting_model",
    "social_media_analytics",
    "email_campaign_analysis",
    "web_traffic_monitoring",
    "database_maintenance",
    "backup_and_recovery",
    "system_health_monitoring",
    "performance_optimization",
    "security_audit_workflow",
    "compliance_reporting",
    "data_quality_assessment",
    "feature_engineering_pipeline",
    "model_training_workflow",
    "hyperparameter_tuning",
    "cross_validation_suite",
    "ensemble_modeling",
    "time_series_forecasting",
    "anomaly_detection_system",
    "clustering_analysis",
    "classification_pipeline",
    "regression_modeling",
    "natural_language_processing",
    "sentiment_analysis_engine",
    "topic_modeling_workflow",
    "document_classification",
    "entity_recognition_pipeline",
    "translation_service",
    "speech_recognition_system",
    "image_classification",
    "object_detection_pipeline",
    "facial_recognition_system",
    "optical_character_recognition",
    "video_processing_workflow",
    "audio_analysis_pipeline",
    "signal_processing_suite",
    "genomics_data_analysis",
    "bioinformatics_pipeline",
    "medical_image_analysis",
    "clinical_trial_management",
    "pharmaceutical_research",
    "chemical_compound_analysis",
    "materials_science_modeling",
    "physics_simulation_suite",
    "astronomy_data_processing",
    "climate_modeling_pipeline",
    "environmental_monitoring",
    "energy_consumption_analysis",
    "smart_grid_optimization",
    "transportation_logistics",
    "supply_chain_optimization",
    "manufacturing_quality_control",
    "predictive_maintenance",
    "resource_allocation_optimizer",
    "scheduling_algorithm_suite",
    "network_traffic_analysis",
    "cybersecurity_monitoring",
    "intrusion_detection_system",
    "vulnerability_assessment",
    "penetration_testing_workflow",
    "digital_forensics_analysis",
    "blockchain_validation",
    "cryptocurrency_analysis",
    "trading_algorithm_suite",
    "risk_assessment_pipeline",
    "portfolio_optimization",
    "credit_scoring_model",
    "loan_approval_workflow",
    "insurance_claim_processing",
    "actuarial_analysis_suite",
    "market_research_pipeline",
    "competitor_analysis_workflow",
    "brand_sentiment_monitoring",
    "social_listening_dashboard",
    "influencer_analysis_suite",
    "content_performance_tracker",
    "seo_optimization_pipeline",
    "website_analytics_processor",
    "conversion_funnel_analysis",
    "user_behavior_tracking",
    "ab_testing_framework",
    "personalization_engine",
    "real_time_recommendation",
    "dynamic_pricing_algorithm",
    "demand_forecasting_model",
    "seasonal_trend_analysis",
    "promotional_impact_assessment"
]

# Schedule intervals for variety
SCHEDULE_INTERVALS = [
    '@once', '@daily', '@hourly', '@weekly', '@monthly',
    timedelta(minutes=5), timedelta(minutes=15), timedelta(minutes=30),
    timedelta(hours=1), timedelta(hours=2), timedelta(hours=4),
    timedelta(hours=6), timedelta(hours=12), timedelta(days=2),
    timedelta(days=3), timedelta(days=7), None
]

# Different default args templates
DEFAULT_ARGS_TEMPLATES = [
    {
        'owner': 'data_team',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    {
        'owner': 'analytics_team',
        'depends_on_past': True,
        'start_date': days_ago(2),
        'email_on_failure': False,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    {
        'owner': 'ml_team',
        'depends_on_past': False,
        'start_date': days_ago(0),
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=15),
    },
    {
        'owner': 'devops_team',
        'depends_on_past': False,
        'start_date': days_ago(3),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
    {
        'owner': 'engineering_team',
        'depends_on_past': True,
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=20),
    }
]

# Task operation types
TASK_OPERATIONS = [
    'fibonacci_calculation',
    'prime_factorization', 
    'matrix_multiplication',
    'monte_carlo_pi',
    'sorting_algorithm',
    'hash_computation',
    'factorial_calculation',
    'power_calculation',
    'trigonometric_series',
    'polynomial_evaluation',
    'xcom_producer',
    'xcom_consumer',
    'file_processing',
    'database_query_simulation',
    'api_call_simulation',
    'data_validation',
    'feature_extraction',
    'model_prediction',
    'aggregation_task',
    'transformation_task',
    'sensor_simulation',
    'email_notification_sim',
    'report_generation',
    'data_quality_check',
    'backup_simulation'
]


def generate_mathematical_operation(operation_type: str, target_time: float, **context) -> Dict[str, Any]:
    """Generate various mathematical operations with controlled execution time."""
    start_time = time.time()
    result = {}
    
    if operation_type == 'fibonacci_calculation':
        count = 0
        a, b = 0, 1
        while time.time() - start_time < target_time:
            a, b = b, a + b
            count += 1
            if count % 1000 == 0:
                time.sleep(0.001)
        result = {'fibonacci_count': count, 'last_value': str(b)[:50]}
        
    elif operation_type == 'prime_factorization':
        numbers_processed = 0
        while time.time() - start_time < target_time:
            num = random.randint(1000, 50000)
            factors = []
            d = 2
            while d * d <= num:
                while num % d == 0:
                    factors.append(d)
                    num //= d
                d += 1
            if num > 1:
                factors.append(num)
            numbers_processed += 1
            time.sleep(0.01)
        result = {'numbers_factored': numbers_processed}
        
    elif operation_type == 'matrix_multiplication':
        multiplications = 0
        while time.time() - start_time < target_time:
            size = random.randint(10, 30)
            matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
            matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]
            
            result_matrix = [[0.0 for _ in range(size)] for _ in range(size)]
            for i in range(size):
                for j in range(size):
                    for k in range(size):
                        result_matrix[i][j] += matrix_a[i][k] * matrix_b[k][j]
            multiplications += 1
        result = {'matrix_multiplications': multiplications}
        
    elif operation_type == 'monte_carlo_pi':
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
        sorts_completed = 0
        while time.time() - start_time < target_time:
            size = random.randint(100, 500)
            arr = [random.randint(1, 1000) for _ in range(size)]
            # Quick sort implementation
            def quicksort(arr):
                if len(arr) <= 1:
                    return arr
                pivot = arr[len(arr) // 2]
                left = [x for x in arr if x < pivot]
                middle = [x for x in arr if x == pivot]
                right = [x for x in arr if x > pivot]
                return quicksort(left) + middle + quicksort(right)
            _ = quicksort(arr)
            sorts_completed += 1
        result = {'arrays_sorted': sorts_completed}
        
    else:  # Default to simple computation
        iterations = 0
        while time.time() - start_time < target_time:
            _ = sum(i**2 for i in range(100))
            iterations += 1
            time.sleep(0.01)
        result = {'iterations_completed': iterations}
    
    actual_time = time.time() - start_time
    result.update({
        'operation_type': operation_type,
        'target_time': target_time,
        'actual_time': actual_time,
        'task_id': context['task_instance'].task_id,
        'dag_id': context['task_instance'].dag_id
    })
    
    return result


def generate_xcom_producer(**context) -> Dict[str, Any]:
    """Produce data for XCom sharing."""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    
    # Generate some data
    data = {
        'timestamp': datetime.now().isoformat(),
        'random_numbers': [random.randint(1, 1000) for _ in range(10)],
        'calculation_result': sum(i**2 for i in range(100)),
        'status': 'completed'
    }
    
    # Simulate processing time
    while time.time() - start_time < target_time:
        _ = sum(random.random() for _ in range(1000))
        time.sleep(0.1)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='shared_data', value=data)
    
    return {
        'operation_type': 'xcom_producer',
        'data_size': len(str(data)),
        'execution_time': time.time() - start_time
    }


def generate_xcom_consumer(**context) -> Dict[str, Any]:
    """Consume data from XCom and process it."""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    
    # Try to pull from XCom (might be None if producer hasn't run)
    shared_data = context['task_instance'].xcom_pull(key='shared_data')
    
    if shared_data:
        # Process the shared data
        processed_sum = sum(shared_data.get('random_numbers', []))
        processed_data = {
            'original_data': shared_data,
            'processed_sum': processed_sum,
            'processing_timestamp': datetime.now().isoformat()
        }
    else:
        # Generate fallback data
        processed_data = {
            'fallback_data': True,
            'generated_value': random.randint(1, 1000),
            'processing_timestamp': datetime.now().isoformat()
        }
    
    # Simulate processing time
    while time.time() - start_time < target_time:
        _ = sum(random.random() for _ in range(500))
        time.sleep(0.1)
    
    return {
        'operation_type': 'xcom_consumer',
        'has_shared_data': shared_data is not None,
        'processed_data': processed_data,
        'execution_time': time.time() - start_time
    }


def generate_file_processing_task(**context) -> Dict[str, Any]:
    """Simulate file processing operations."""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    
    # Simulate file operations
    file_count = random.randint(5, 50)
    processed_files = []
    
    while time.time() - start_time < target_time:
        for i in range(min(file_count, 10)):  # Process in batches
            filename = f"data_file_{random.randint(1000, 9999)}.csv"
            file_size = random.randint(1024, 1024*1024)  # 1KB to 1MB
            processed_files.append({
                'filename': filename,
                'size': file_size,
                'checksum': hash(filename + str(file_size))
            })
        time.sleep(0.5)
        if len(processed_files) >= file_count:
            break
    
    return {
        'operation_type': 'file_processing',
        'files_processed': len(processed_files),
        'total_size': sum(f['size'] for f in processed_files),
        'execution_time': time.time() - start_time
    }


def generate_database_simulation(**context) -> Dict[str, Any]:
    """Simulate database operations."""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    
    operations = []
    operation_types = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
    
    while time.time() - start_time < target_time:
        op_type = random.choice(operation_types)
        records_affected = random.randint(1, 1000)
        execution_time = random.uniform(0.1, 2.0)
        
        operations.append({
            'operation': op_type,
            'records': records_affected,
            'duration': execution_time
        })
        
        # Simulate query execution time
        time.sleep(execution_time)
    
    return {
        'operation_type': 'database_simulation',
        'operations_count': len(operations),
        'total_records': sum(op['records'] for op in operations),
        'execution_time': time.time() - start_time
    }


def generate_api_simulation(**context) -> Dict[str, Any]:
    """Simulate API calls."""
    target_time = random.uniform(5, 300)
    start_time = time.time()
    
    api_calls = []
    endpoints = ['/users', '/orders', '/products', '/analytics', '/reports']
    
    while time.time() - start_time < target_time:
        endpoint = random.choice(endpoints)
        response_time = random.uniform(0.1, 3.0)
        status_code = random.choices([200, 201, 400, 404, 500], weights=[70, 10, 10, 5, 5])[0]
        
        api_calls.append({
            'endpoint': endpoint,
            'response_time': response_time,
            'status_code': status_code,
            'timestamp': time.time()
        })
        
        time.sleep(response_time)
    
    successful_calls = sum(1 for call in api_calls if call['status_code'] < 400)
    
    return {
        'operation_type': 'api_simulation',
        'total_calls': len(api_calls),
        'successful_calls': successful_calls,
        'success_rate': successful_calls / len(api_calls) if api_calls else 0,
        'execution_time': time.time() - start_time
    }


def create_task(task_id: str, operation_type: str, dag: DAG) -> PythonOperator:
    """Create a task based on the operation type."""
    target_time = random.uniform(5, 300)  # 5 seconds to 5 minutes
    
    if operation_type == 'xcom_producer':
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_xcom_producer,
            dag=dag
        )
    elif operation_type == 'xcom_consumer':
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_xcom_consumer,
            dag=dag
        )
    elif operation_type == 'file_processing':
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_file_processing_task,
            dag=dag
        )
    elif operation_type == 'database_query_simulation':
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_database_simulation,
            dag=dag
        )
    elif operation_type == 'api_call_simulation':
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_api_simulation,
            dag=dag
        )
    elif operation_type in ['sensor_simulation', 'email_notification_sim', 'report_generation']:
        # Use Bash operators for some variety
        bash_commands = {
            'sensor_simulation': f'echo "Simulating sensor reading..." && sleep {int(target_time)} && echo "Sensor data: $(date)"',
            'email_notification_sim': f'echo "Sending email notification..." && sleep {int(target_time)} && echo "Email sent successfully"',
            'report_generation': f'echo "Generating report..." && sleep {int(target_time)} && echo "Report generated: report_$(date +%s).pdf"'
        }
        
        return BashOperator(
            task_id=task_id,
            bash_command=bash_commands.get(operation_type, f'sleep {int(target_time)}'),
            dag=dag
        )
    else:
        # Mathematical operations
        return PythonOperator(
            task_id=task_id,
            python_callable=generate_mathematical_operation,
            op_kwargs={
                'operation_type': operation_type,
                'target_time': target_time
            },
            dag=dag
        )


def generate_dag_structure(num_tasks: int) -> List[Tuple[str, List[str], str]]:
    """Generate DAG structure with tasks, dependencies, and operation types."""
    tasks = []
    task_names = [f"task_{i:03d}" for i in range(num_tasks)]
    
    # Assign operation types to tasks
    for i in range(num_tasks):
        operation_type = random.choice(TASK_OPERATIONS)
        
        if i == 0:
            # First task has no dependencies
            tasks.append((task_names[i], [], operation_type))
        else:
            # Determine number of dependencies (1-4 sequential tasks)
            max_deps = min(4, i)
            num_deps = random.randint(1, max_deps)
            
            # Select dependencies from previous tasks
            possible_deps = task_names[:i]
            dependencies = random.sample(possible_deps, min(num_deps, len(possible_deps)))
            
            tasks.append((task_names[i], dependencies, operation_type))
    
    return tasks


def create_single_dag(dag_number: int) -> DAG:
    """Create a single DAG with unique configuration."""
    # Select unique name and configuration
    dag_name_base = DAG_NAME_TEMPLATES[dag_number % len(DAG_NAME_TEMPLATES)]
    dag_id = f"{dag_name_base}_{dag_number:03d}"
    
    # Random configuration
    num_tasks = random.randint(2, 100)
    default_args = random.choice(DEFAULT_ARGS_TEMPLATES).copy()
    schedule_interval = random.choice(SCHEDULE_INTERVALS)
    catchup = random.choice([True, False])
    max_active_runs = random.choice([1, 2, 3, 5, 10])
    concurrency = random.choice([16, 32, 64, 128])
    
    # Vary start dates
    start_date_offset = random.randint(0, 10)
    default_args['start_date'] = days_ago(start_date_offset)
    
    # Create DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Benchmark DAG: {dag_name_base.replace("_", " ").title()} with {num_tasks} tasks',
        schedule_interval=schedule_interval,
        catchup=catchup,
        max_active_runs=max_active_runs,
        concurrency=concurrency,
        tags=['benchmark', '100dags', dag_name_base.split('_')[0]],
    )
    
    # Generate task structure
    task_structure = generate_dag_structure(num_tasks)
    
    # Create tasks
    tasks = {}
    for task_id, dependencies, operation_type in task_structure:
        task = create_task(task_id, operation_type, dag)
        tasks[task_id] = task
    
    # Set up dependencies
    for task_id, dependencies, _ in task_structure:
        if dependencies:
            for dep in dependencies:
                if dep in tasks and task_id in tasks:
                    tasks[dep] >> tasks[task_id]
    
    return dag


# Generate all 100 DAGs
print("Generating 100 unique DAGs...")
generated_dags = {}

for i in range(100):
    try:
        dag = create_single_dag(i)
        generated_dags[dag.dag_id] = dag
        if (i + 1) % 10 == 0:
            print(f"Generated {i + 1} DAGs...")
    except Exception as e:
        print(f"Error creating DAG {i}: {str(e)}")
        continue

# Add DAGs to globals for Airflow discovery
globals().update(generated_dags)
print(f"Successfully generated {len(generated_dags)} DAGs")
