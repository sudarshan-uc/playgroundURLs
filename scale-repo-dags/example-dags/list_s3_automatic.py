from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
import botocore
import hashlib

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}

default_conn_id = "aws-assumerole-test"

dag = DAG(
    dag_id="list_s3_buckets_automatic",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # or schedule=None on newer Airflow
    catchup=False,
    tags=["s3", "example"],
)

def _bucket_name(context, prefix: str = "test-bucket") -> str:
    # Stable per-run; S3-safe (lowercase hex, hyphens)
    run_id = context["run_id"]
    suffix = hashlib.md5(run_id.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}-{suffix}"

def list_buckets_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    resp = client.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]
    print("S3 buckets:", names)

list_buckets = PythonOperator(
    task_id="list_s3_buckets_task",
    python_callable=list_buckets_fn,
    dag=dag,
)

def create_bucket_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    region = getattr(client.meta, "region_name", None)
    bucket = _bucket_name(context)
    # Push the bucket name so downstream tasks reuse the same name
    context['ti'].xcom_push(key='bucket_name', value=bucket)
    try:
        if region and region != "us-east-1":
            client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            client.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket} in region {region}")
    except client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {bucket} already owned by you")
    except client.exceptions.BucketAlreadyExists:
        print(f"Bucket {bucket} already exists (global namespace).")

create_bucket = PythonOperator(
    task_id="create_test_bucket_task",
    python_callable=create_bucket_fn,
    dag=dag,
)

def put_object_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    # Pull the bucket name created earlier to ensure all tasks use same bucket
    bucket = context['ti'].xcom_pull(key='bucket_name', task_ids='create_test_bucket_task')
    if not bucket:
        raise RuntimeError('Bucket name not found in XCom from create_test_bucket_task')
    client.put_object(Bucket=bucket, Key="test.txt", Body="This is a test file.")
    print(f"Wrote test object to {bucket}/test.txt")

put_object = PythonOperator(
    task_id="put_object_in_test_bucket_task",
    python_callable=put_object_fn,
    dag=dag,
)

def delete_bucket_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    # Pull the bucket name created earlier to ensure we delete the same bucket
    bucket = context['ti'].xcom_pull(key='bucket_name', task_ids='create_test_bucket_task')
    if not bucket:
        print('Bucket name not found in XCom; skipping delete')
        return
    try:
        # Delete objects (simple, non-versioned cleanup)
        objs = client.list_objects_v2(Bucket=bucket)
        if objs.get("KeyCount", 0) > 0:
            to_delete = {"Objects": [{"Key": o["Key"]} for o in objs.get("Contents", [])]}
            client.delete_objects(Bucket=bucket, Delete=to_delete)
        client.delete_bucket(Bucket=bucket)
        print(f"Deleted bucket: {bucket}")
    except botocore.exceptions.ClientError as e:
        print(f"Error deleting bucket {bucket}: {e}")

delete_bucket = PythonOperator(
    task_id="delete_test_bucket_task",
    python_callable=delete_bucket_fn,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

list_buckets >> create_bucket >> put_object >> delete_bucket
