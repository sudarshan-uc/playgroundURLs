from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

try:  # pragma: no cover - optional in some test envs
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:  # noqa: BLE001
    boto3 = None

AWS_CONN_ID = "aws-assumerole-test"


def get_aws_client(service_name: str):
    """Return a boto3 client for service using the Airflow connection.

    Requires apache-airflow-providers-amazon package installed.
    """
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Install 'apache-airflow-providers-amazon' to use the AWS connection."
        ) from exc
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type=service_name)
    return hook.get_client_type(service_name)

log = logging.getLogger(__name__)


def check_boto3():
    if boto3 is None:
        raise RuntimeError(
            "boto3 is not installed in the Airflow environment. Add 'boto3' to requirements.txt or install it."
        )


def get_caller_identity(**_context) -> dict:
    sts = get_aws_client("sts")
    try:
        resp = sts.get_caller_identity()
        log.info("STS get-caller-identity: %s", resp)
        return resp
    except (BotoCoreError, ClientError) as e:
        log.exception("Failed to call STS: %s", e)
        raise


def list_s3_buckets(**_context) -> list:
    s3 = get_aws_client("s3")
    try:
        resp = s3.list_buckets()
        buckets = [b.get("Name") for b in resp.get("Buckets", [])]
        log.info("S3 buckets: %s", buckets)
        return buckets
    except (BotoCoreError, ClientError) as e:
        log.exception("Failed to list S3 buckets: %s", e)
        raise


def print_injected_env_and_config(**_context):
    # Print environment variables that are commonly injected by pod-identity
    import os

    keys_of_interest = [
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]

    log.info("--- START injected env/config dump ---")
    for k in keys_of_interest:
        log.info("%s=%s", k, os.environ.get(k))

    # Print all env vars that contain 'AWS' (case-insensitive) to be thorough
    for k, v in sorted(os.environ.items()):
        if "AWS" in k.upper():
            log.info("%s=%s", k, v)

    # Show boto3 session info if available
    if boto3 is not None:
        try:
            session = boto3.session.Session()
            creds = session.get_credentials()
            frozen = creds.get_frozen_credentials() if creds else None
            log.info("boto3.session.region_name=%s", session.region_name)
            if frozen:
                log.info(
                    "boto3 credentials access_key=%s, secret_key_present=%s, token_present=%s",
                    frozen.access_key,
                    bool(frozen.secret_key),
                    bool(frozen.token),
                )
        except Exception as e:
            log.exception("Error while inspecting boto3 session: %s", e)
    else:
        log.info("boto3 not available; cannot show session/credentials info")


DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

with DAG(
    dag_id="cloud_identity_and_storage",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["cloud", "example"],
) as dag:

    # Initial dummy starter task so any validations/actions begin at second task
    starter = PythonOperator(
        task_id="starter_noop_task",
        python_callable=lambda **ctx: log.info("Starter task completed"),
        provide_context=True,
    )

    printer = PythonOperator(
        task_id="print_injected_config_task",
        python_callable=print_injected_env_and_config,
        provide_context=True,
    )

    task_identity = PythonOperator(
        task_id="get_identity_task",
        python_callable=get_caller_identity,
        provide_context=True,
    )

    task_storage = PythonOperator(
        task_id="list_storage_buckets_task",
        python_callable=list_s3_buckets,
        provide_context=True,
    )

    # Order: starter -> printer -> identity -> storage
    starter >> printer >> task_identity >> task_storage