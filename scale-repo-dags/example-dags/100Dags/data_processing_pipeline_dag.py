"""
Data Processing Pipeline DAG - ETL workflow with XCom
"""
import random
import time
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def extract_data(**context):
    """Extract simulated data"""
    time.sleep(random.uniform(10, 120))
    data = {
        'records': [{'id': i, 'value': random.randint(1, 1000)} for i in range(100)],
        'timestamp': str(context['ds']),
        'source': 'data_warehouse'
    }
    context['task_instance'].xcom_push(key='extracted_data', value=data)
    return f"Extracted {len(data['records'])} records"

def transform_data(**context):
    """Transform the extracted data"""
    time.sleep(random.uniform(20, 180))
    task_instance = context['task_instance']
    raw_data = task_instance.xcom_pull(task_ids='extract_data', key='extracted_data')
    
    if raw_data:
        transformed_records = []
        for record in raw_data['records']:
            transformed_records.append({
                'id': record['id'],
                'normalized_value': record['value'] / 1000.0,
                'category': 'high' if record['value'] > 500 else 'low'
            })
        
        transformed_data = {
            'records': transformed_records,
            'transformation_timestamp': str(context['ds']),
            'total_records': len(transformed_records)
        }
        
        context['task_instance'].xcom_push(key='transformed_data', value=transformed_data)
        return f"Transformed {len(transformed_records)} records"
    return "No data to transform"

def validate_data(**context):
    """Validate transformed data"""
    time.sleep(random.uniform(5, 60))
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    if data and data['records']:
        validation_results = {
            'total_records': len(data['records']),
            'high_category_count': sum(1 for r in data['records'] if r['category'] == 'high'),
            'low_category_count': sum(1 for r in data['records'] if r['category'] == 'low'),
            'validation_passed': True
        }
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        return validation_results
    return {'validation_passed': False}

def load_data(**context):
    """Load data to destination"""
    time.sleep(random.uniform(15, 120))
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(task_ids='transform_data', key='transformed_data')
    validation = task_instance.xcom_pull(task_ids='validate_data', key='validation_results')
    
    if validation and validation.get('validation_passed'):
        return f"Successfully loaded {data['total_records']} records"
    return "Load failed - validation did not pass"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'data_processing_pipeline_dag',
    default_args=default_args,
    description='ETL data processing pipeline with XCom',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=8,
    tags=['etl', 'data_processing', 'xcom'],
)

# ETL Pipeline tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Monitoring task
monitor_task = BashOperator(
    task_id='monitor_pipeline',
    bash_command='echo "Pipeline completed successfully" && sleep 10',
    dag=dag,
)

# Set dependencies - ETL pipeline flow
extract_task >> transform_task >> validate_task >> load_task >> monitor_task
