from __future__ import annotations

from datetime import datetime
import logging


from airflow import DAG

from airflow.operators.bash import BashOperator


log = logging.getLogger(__name__)


DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}


with DAG(
    dag_id="print_cloud_debug_info",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["cloud", "example"],
) as dag:


    # Task to print selected environment variables for debugging
    print_vars_task = BashOperator(
        task_id="print_vars_task",
        bash_command="env | grep -i AWS || true",
    )


    # Task to run 'aws sts get-caller-identity' for identity debugging (aws cli must be available in the image)
    caller_identity_task = BashOperator(
        task_id="caller_identity_task",
        bash_command="aws sts get-caller-identity || true",
    )

    print_vars_task >> caller_identity_task

    # Task that sleeps for 1 day
    task_sleep_1d = BashOperator(
        task_id="sleep_1d_task",
        bash_command="sleep 1d",
    )
