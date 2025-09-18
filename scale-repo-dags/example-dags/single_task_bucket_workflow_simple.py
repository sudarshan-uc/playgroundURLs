from __future__ import annotations

from datetime import datetime
import time
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import botocore

CONN_ID = "aws-assumerole-test"

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}


def workflow_simple(**context):
    hook = S3Hook(aws_conn_id=CONN_ID)
    client = hook.get_conn()

    bucket_name = f"simple-single-task-bucket-{uuid.uuid4().hex[:8]}"
    print("Creating bucket:", bucket_name)
    try:
        region = getattr(client.meta, "region_name", None)
        if region and region != "us-east-1":
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            client.create_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        print("Create bucket failed:", e)
        raise

    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key1 = f"file-{now}.txt"
    client.put_object(Bucket=bucket_name, Key=key1, Body=f"Created at {now}")
    print("Wrote object:", key1)

    # sleep 15 minutes
    print("Sleeping 15 minutes")
    time.sleep(30 * 60)

    now2 = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key2 = f"file-{now2}.txt"
    client.put_object(Bucket=bucket_name, Key=key2, Body=f"Created at {now2}")
    print("Wrote object:", key2)

    # sleep 30 minutes
    print("Sleeping 30 minutes")
    time.sleep(40 * 60)

    resp = client.list_objects_v2(Bucket=bucket_name)
    contents = [o["Key"] for o in resp.get("Contents", [])]
    print("Bucket contents:", contents)

    if contents:
        delete_payload = {"Objects": [{"Key": k} for k in contents]}
        client.delete_objects(Bucket=bucket_name, Delete=delete_payload)
    client.delete_bucket(Bucket=bucket_name)
    print("Deleted bucket:", bucket_name)


with DAG(
    dag_id="single_task_bucket_workflow_simple",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["s3", "example"],
) as dag:

    run_simple = PythonOperator(
        task_id="run_simple_workflow",
        python_callable=workflow_simple,
    )
