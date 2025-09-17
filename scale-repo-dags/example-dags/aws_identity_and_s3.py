from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:  # pragma: no cover - boto3 may not be installed in test env
    boto3 = None

log = logging.getLogger(__name__)


def check_boto3():
    if boto3 is None:
        raise RuntimeError(
            "boto3 is not installed in the Airflow environment. Add 'boto3' to requirements.txt or install it."
        )


def get_caller_identity(**_context) -> dict:
    check_boto3()
    sts = boto3.client("sts")
    try:
        resp = sts.get_caller_identity()
        log.info("STS get-caller-identity: %s", resp)
        return resp
    except (BotoCoreError, ClientError) as e:
        log.exception("Failed to call STS: %s", e)
        raise


def list_s3_buckets(**_context) -> list:
    check_boto3()
    s3 = boto3.client("s3")
    try:
        resp = s3.list_buckets()
        buckets = [b.get("Name") for b in resp.get("Buckets", [])]
        log.info("S3 buckets: %s", buckets)
        return buckets
    except (BotoCoreError, ClientError) as e:
        log.exception("Failed to list S3 buckets: %s", e)
        raise


DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

with DAG(
    dag_id="aws_identity_and_s3",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["aws", "example"],
) as dag:

    task_get_identity = PythonOperator(
        task_id="get_caller_identity",
        python_callable=get_caller_identity,
        provide_context=True,
    )

    task_list_buckets = PythonOperator(
        task_id="list_s3_buckets",
        python_callable=list_s3_buckets,
        provide_context=True,
    )

    task_get_identity >> task_list_buckets
