"""
Machine Learning Pipeline DAG - ML workflow simulation
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def data_preprocessing(**context):
    """Simulate data preprocessing for ML"""
    time.sleep(random.uniform(30, 180))
    
    # Simulate data cleaning and feature engineering
    features_processed = random.randint(50, 500)
    samples_processed = random.randint(1000, 10000)
    
    result = {
        'features': features_processed,
        'samples': samples_processed,
        'preprocessing_complete': True
    }
    context['task_instance'].xcom_push(key='preprocessing_result', value=result)
    return result

def model_training(**context):
    """Simulate ML model training"""
    time.sleep(random.uniform(60, 300))
    
    task_instance = context['task_instance']
    preprocessing_data = task_instance.xcom_pull(task_ids='data_preprocessing', key='preprocessing_result')
    
    if preprocessing_data:
        # Simulate training iterations
        epochs = random.randint(10, 100)
        accuracy = random.uniform(0.75, 0.98)
        
        result = {
            'epochs_trained': epochs,
            'final_accuracy': accuracy,
            'model_trained': True
        }
        context['task_instance'].xcom_push(key='training_result', value=result)
        return result
    return {'model_trained': False}

def model_evaluation(**context):
    """Simulate model evaluation"""
    time.sleep(random.uniform(20, 120))
    
    task_instance = context['task_instance']
    training_data = task_instance.xcom_pull(task_ids='model_training', key='training_result')
    
    if training_data and training_data.get('model_trained'):
        test_accuracy = training_data['final_accuracy'] * random.uniform(0.85, 1.05)
        precision = random.uniform(0.7, 0.95)
        recall = random.uniform(0.7, 0.95)
        
        result = {
            'test_accuracy': min(test_accuracy, 1.0),
            'precision': precision,
            'recall': recall,
            'evaluation_complete': True
        }
        context['task_instance'].xcom_push(key='evaluation_result', value=result)
        return result
    return {'evaluation_complete': False}

default_args = {
    'owner': 'ml_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'machine_learning_pipeline_dag',
    default_args=default_args,
    description='Machine learning training and evaluation pipeline',
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    concurrency=8,
    tags=['machine_learning', 'ml', 'training'],
)

# Data preparation
data_prep = PythonOperator(
    task_id='data_preprocessing',
    python_callable=data_preprocessing,
    dag=dag,
)

# Model training
training = PythonOperator(
    task_id='model_training',
    python_callable=model_training,
    dag=dag,
)

# Model evaluation
evaluation = PythonOperator(
    task_id='model_evaluation',
    python_callable=model_evaluation,
    dag=dag,
)

# Model deployment simulation
deployment = BashOperator(
    task_id='model_deployment',
    bash_command='echo "Deploying ML model to production" && sleep 30',
    dag=dag,
)

# Set ML pipeline dependencies
data_prep >> training >> evaluation >> deployment
