from __future__ import annotations

from datetime import datetime
import uuid
import re
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
import botocore

# Configuration
CONN_ID = "aws-assumerole-test"

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}


def make_role_session_name(context: dict) -> str:
    ti = context.get("ti")
    dag_id = ti.dag_id if ti is not None else context.get("dag", {}).get("dag_id", "")
    task_id = ti.task_id if ti is not None else context.get("task", {}).get("task_id", "")
    run_id = (context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""))
    base = f"airflow-{dag_id}-{task_id}-{run_id}"
    sanitized = re.sub(r"[^A-Za-z0-9+=,.@-]", "-", base)
    return sanitized[:64]


def get_boto3_session_from_conn(context: dict):
    conn = BaseHook.get_connection(CONN_ID)
    extras = conn.extra_dejson or {}
    region = extras.get("region_name")
    role_arn = extras.get("role_arn")

    if not role_arn:
        return boto3.Session(region_name=region), region, None

    assume_kwargs = extras.get("assume_role_kwargs") or {}
    if not isinstance(assume_kwargs, dict):
        assume_kwargs = {}

    assume_kwargs = dict(assume_kwargs)
    assume_kwargs["RoleSessionName"] = make_role_session_name(context)

    sts_client = boto3.client("sts", region_name=region)
    resp = sts_client.assume_role(RoleArn=role_arn, **assume_kwargs)
    creds = resp["Credentials"]
    session = boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )
    return session, region, creds


def workflow_task(**context):
    # Assume role once and keep session for entire long-running task
    session, region, creds = get_boto3_session_from_conn(context)
    if creds:
        exp = creds.get("Expiration")
        print(f"Assumed role credentials expire at: {exp}")
    else:
        print("No assume-role used; running with default credentials")

    s3 = session.client("s3", region_name=region)

    bucket_name = f"single-task-bucket-{uuid.uuid4().hex[:8]}"
    print("Creating bucket:", bucket_name)
    try:
        if region and region != "us-east-1":
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            s3.create_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        print("Create bucket failed:", e)
        raise

    # first file
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key1 = f"file-{now}.txt"
    s3.put_object(Bucket=bucket_name, Key=key1, Body=f"Created at {now}")
    print("Wrote object:", key1)

    # sleep 15 minutes to test token expiry
    print("Sleeping 15 minutes")
    time.sleep(30 * 60)

    # attempt second write with same session (no refresh)
    now2 = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key2 = f"file-{now2}.txt"
    try:
        s3.put_object(Bucket=bucket_name, Key=key2, Body=f"Created at {now2}")
        print("Wrote object:", key2)
    except botocore.exceptions.ClientError as e:
        print("Second put_object failed (likely token expiry):", e)

    # sleep another 30 minutes
    print("Sleeping 30 minutes")
    time.sleep(40 * 60)

    # list and delete
    try:
        resp = s3.list_objects_v2(Bucket=bucket_name)
        contents = [o["Key"] for o in resp.get("Contents", [])]
        print("Bucket contents:", contents)
        if contents:
            delete_payload = {"Objects": [{"Key": k} for k in contents]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_payload)
    except botocore.exceptions.ClientError as e:
        print("List/delete failed:", e)

    try:
        s3.delete_bucket(Bucket=bucket_name)
        print("Deleted bucket:", bucket_name)
    except botocore.exceptions.ClientError as e:
        print("Delete bucket failed:", e)


with DAG(
    dag_id="single_task_bucket_workflow",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["s3", "example"],
) as dag:

    run_workflow = PythonOperator(
        task_id="run_workflow_task",
        python_callable=workflow_task,
    )
