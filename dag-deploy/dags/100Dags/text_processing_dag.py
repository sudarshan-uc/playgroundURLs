"""
Text Processing DAG - Natural language processing
"""
import random
import time
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def tokenize_text(**context):
    """Simulate text tokenization"""
    target_time = random.uniform(10, 120)
    start_time = time.time()
    documents_processed = 0
    
    while time.time() - start_time < target_time:
        # Simulate tokenization
        doc_length = random.randint(100, 10000)  # words
        processing_time = doc_length / 10000  # seconds per word
        time.sleep(min(processing_time, 1.0))
        documents_processed += 1
    
    result = {'documents_tokenized': documents_processed}
    context['task_instance'].xcom_push(key='tokenization_result', value=result)
    return result

def sentiment_analysis(**context):
    """Perform sentiment analysis"""
    time.sleep(random.uniform(30, 200))
    
    sentiments = ['positive', 'negative', 'neutral']
    sentiment_scores = {
        'positive': random.randint(10, 100),
        'negative': random.randint(5, 50),
        'neutral': random.randint(20, 80)
    }
    
    return {'sentiment_analysis_complete': True, 'sentiment_scores': sentiment_scores}

default_args = {
    'owner': 'nlp_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'text_processing_dag',
    default_args=default_args,
    description='Natural language processing and text analysis',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=2,
    concurrency=16,
    tags=['nlp', 'text_processing', 'sentiment'],
)

# Text processing pipeline
tokenize_tasks = []
for i in range(1, 12):
    task = PythonOperator(
        task_id=f'tokenize_batch_{i}',
        python_callable=tokenize_text,
        dag=dag,
    )
    tokenize_tasks.append(task)

sentiment_task = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=sentiment_analysis,
    dag=dag,
)

# Dependencies
tokenize_tasks[0] >> tokenize_tasks[1] >> sentiment_task
tokenize_tasks[2] >> tokenize_tasks[3] >> sentiment_task
