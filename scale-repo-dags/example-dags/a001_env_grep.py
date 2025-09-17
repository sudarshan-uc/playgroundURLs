from __future__ import annotations

from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

with DAG(
    dag_id="print_cloud_vars",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["cloud", "example"],
) as dag:

    # Runs the shell command that lists environment variables and filters for cloud-related ones
    task_cloud_var_grep = BashOperator(
        task_id="grep_cloud_vars_task",
        bash_command="env | grep -i AWS || true",
    )

    # Task that sleeps for 1 day
    task_sleep_1d = BashOperator(
        task_id="sleep_1d_task",
        bash_command="sleep 1d",
    )
