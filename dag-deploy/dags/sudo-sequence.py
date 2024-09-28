from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "I am task 1, I am successful"',
    )
    
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "I am task 2, I am successful"',
    )
    
    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "I am task 3, I am successful"',
    )
    
    task4 = BashOperator(
        task_id='task4',
        bash_command='echo "I am task 4, I am successful"',
        trigger_rule='one_success',  # This ensures task4 runs if any of task1, task2, or task3 succeed
    )
    
    task5 = BashOperator(
        task_id='task5',
        bash_command='echo "I am task 5, I am successful"',
    )
    
    task6 = BashOperator(
        task_id='task6',
        bash_command='echo "I am task 6, I am successful"',
    )
    
    [task1, task2, task3] >> task4
    task4 >> [task5, task6]
