from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def branch_task():
    task_source = Variable.get("TASK_SOURCE", default_var="invalid")
    if task_source == "1":
        return "task1"
    elif task_source == "2":
        return "task2"
    elif task_source == "3":
        return "task3"
    elif task_source == "0":
        return ["task1", "task2", "task3"]
    else:
        return "no_task"

with DAG(
    'sudo-sequence-test',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    #start = EmptyOperator(task_id='start')
    
    #branch = BranchPythonOperator(
    #    task_id='branch_task',
    #    python_callable=branch_task,
    #)
    
    task1 = BashOperator(
        task_id='task1',
        #bash_command='echo "I am task 1, I am successful"',
        bash_command='sleep 60',
    )
    
    task2 = BashOperator(
        task_id='task2',
        #bash_command='echo "I am task 2, I am successful"',
        bash_command='sleep 120',
    )
    
    task3 = BashOperator(
        task_id='task3',
        #bash_command='echo "I am task 3, I am successful"',
        bash_command='sleep 180',
    )
    
    no_task = EmptyOperator(task_id='no_task')
    
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
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    
    start >> branch
    branch >> [task1, task2, task3, no_task]
    [task1, task2, task3] >> task4
    task4 >> [task5, task6] >> end
    no_task >> end
