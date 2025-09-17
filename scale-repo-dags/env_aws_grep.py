from __future__ import annotations

from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

with DAG(
    dag_id="env_aws_grep",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["env", "aws"],
) as dag:

    # Runs the shell command that lists environment variables and filters for AWS-related ones
    task_env_grep = BashOperator(
        task_id="env_grep_aws",
        bash_command="env | grep -i AWS || true",
    )
