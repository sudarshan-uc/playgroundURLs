from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

try:  # pragma: no cover - optional in some test envs
    import boto3
except Exception:  # noqa: BLE001
    boto3 = None

AWS_CONN_ID = "aws-assumerole-test"


def list_buckets_via_conn():  # no context needed
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="s3")
    s3 = hook.get_client_type("s3")
    resp = s3.list_buckets()
    buckets = [b.get("Name") for b in resp.get("Buckets", [])]
    print("Buckets:", buckets)
    return buckets


DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

with DAG(
    dag_id="cloud_identity_and_storage",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["cloud", "example"],
) as dag:

    sts_identity = BashOperator(
        task_id="aws_sts_get_caller_identity",
        bash_command="aws sts get-caller-identity",
    )

    list_buckets = PythonOperator(
        task_id="list_s3_buckets",
        python_callable=list_buckets_via_conn,
    )

    sts_identity >> list_buckets