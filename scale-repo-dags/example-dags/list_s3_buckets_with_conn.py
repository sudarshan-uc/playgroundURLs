from __future__ import annotations

from datetime import datetime
"""
DAG to demonstrate listing S3 buckets, creating a test bucket, writing to it, and deleting it, using an Airflow connection
with assume role parameters in the 'extra' field (for cross-account access).

The Airflow connection (e.g., 'ps-aws-config') should be of type 'Amazon Web Services' and have the following in the 'Extra' field:
{
    "role_arn": "arn:aws:iam::<account-id>:role/<role-name>",
    "external_id": "<optional-external-id>"
}

With Session details in extras field:

{
  "region_name": "us-east-2",
  "role_arn": "arn:aws:iam::833664315823:role/assumerole-test-cross-account",
  "assume_role_method": "assume_role",
  "assume_role_kwargs": {
    "ExternalId": "us-east-2/043672736276/gsd-rdc-01/astronomer-git-test/my-aws-test",
    "RoleSessionName": "airflow-{{ ti.dag_id }}",
    "DurationSeconds": 3600
  }
}



This allows Airflow to assume the specified role in another AWS account for all AWS operators in this DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
import uuid
import botocore
import boto3
import re
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}



# Use an Airflow connection with assume role parameters in 'extra' for cross-account access
default_conn_id = "aws-assumerole-test"


def generate_bucket_name_fn(**context):
    name = f"test-bucket-{uuid.uuid4().hex[:8]}"
    print(f"Generated bucket name: {name}")
    return name


def make_role_session_name(context: dict) -> str:
    """Build a sanitized RoleSessionName allowed by AWS (chars: \w+=,.@-), max 64 chars.

    Uses DAG id, task id and run id (or timestamp) to make a unique name per task/run.
    """
    ti = context.get("ti")
    dag_id = ti.dag_id if ti is not None else context.get("dag", {}).get("dag_id", "")
    task_id = ti.task_id if ti is not None else context.get("task", {}).get("task_id", "")
    run_id = (context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""))
    base = f"airflow-{dag_id}-{task_id}-{run_id}"
    # replace disallowed chars with '-'
    sanitized = re.sub(r"[^A-Za-z0-9+=,.@-]", "-", base)
    # trim to 64 chars
    return sanitized[:64]


def get_boto3_session_from_conn(context: dict):
    """Return a boto3.Session and region using connection extras. If role_arn present, assume role with a dynamic RoleSessionName.

    The connection extras should contain 'role_arn' and optional 'assume_role_kwargs'.
    """
    conn = BaseHook.get_connection(default_conn_id)
    extras = conn.extra_dejson or {}
    region = extras.get("region_name")
    role_arn = extras.get("role_arn")

    if not role_arn:
        # No assume-role configured — fall back to normal session
        return boto3.Session(region_name=region), region

    assume_kwargs = extras.get("assume_role_kwargs") or {}
    if not isinstance(assume_kwargs, dict):
        assume_kwargs = {}

    # ensure RoleSessionName is dynamic and valid
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
    return session, region

dag = DAG(
    dag_id="list_s3_buckets_manually"
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["s3", "example"],
)



# List all S3 buckets using the connection (with assume role if configured)
def list_buckets_fn(**context):
    session, region = get_boto3_session_from_conn(context)
    client = session.client("s3", region_name=region)
    resp = client.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]
    print("S3 buckets:", names)


list_buckets = PythonOperator(
    task_id="list_s3_buckets_task",
    python_callable=list_buckets_fn,
    dag=dag,
)


# Create a test bucket in the assumed role's account
def create_bucket_fn(**context):
    ti = context["ti"]
    bucket_name = ti.xcom_pull(task_ids="generate_bucket_name_task")
    session, region = get_boto3_session_from_conn(context)
    client = session.client("s3", region_name=region)
    try:
        if region and region != "us-east-1":
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name} in region {region}")
    except client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {bucket_name} already owned by you")


create_bucket = PythonOperator(
    task_id="create_test_bucket_task",
    python_callable=create_bucket_fn,
    dag=dag,
)


# Write an object to the test bucket
def put_object_fn(**context):
    ti = context["ti"]
    bucket_name = ti.xcom_pull(task_ids="generate_bucket_name_task")
    session, region = get_boto3_session_from_conn(context)
    client = session.client("s3", region_name=region)
    client.put_object(Bucket=bucket_name, Key="test.txt", Body="This is a test file.")
    print(f"Wrote test object to {bucket_name}/test.txt")


put_object = PythonOperator(
    task_id="put_object_in_test_bucket_task",
    python_callable=put_object_fn,
    dag=dag,
)


def delete_bucket_fn(**context):
    ti = context["ti"]
    bucket_name = ti.xcom_pull(task_ids="generate_bucket_name_task")
    session, region = get_boto3_session_from_conn(context)
    client = session.client("s3", region_name=region)
    # delete all objects first
    try:
        objs = client.list_objects_v2(Bucket=bucket_name)
        if objs.get("KeyCount", 0) > 0:
            to_delete = {"Objects": [{"Key": o["Key"]} for o in objs.get("Contents", [])]}
            client.delete_objects(Bucket=bucket_name, Delete=to_delete)
        client.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")
    except botocore.exceptions.ClientError as e:
        print(f"Error deleting bucket {bucket_name}: {e}")


delete_bucket = PythonOperator(
    task_id="delete_test_bucket_task",
    python_callable=delete_bucket_fn,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

generate_bucket = PythonOperator(
    task_id="generate_bucket_name_task",
    python_callable=generate_bucket_name_fn,
    dag=dag,
)

list_buckets >> generate_bucket >> create_bucket >> put_object >> delete_bucket
