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
This allows Airflow to assume the specified role in another AWS account for all AWS operators in this DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
import uuid
import botocore

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}



# Use an Airflow connection with assume role parameters in 'extra' for cross-account access
default_conn_id = "aws-assumerole-test"
test_bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"

# Example connection 'Extra' JSON to configure cross-account assume-role access:
# {
#   "region_name": "ap-south-1",
#   "role_arn": "arn:aws:iam::222222222222:role/airflow-xacct-marketing",
#   "assume_role_method": "assume_role",
#   "assume_role_kwargs": {
#     "ExternalId": "your-external-id",
#     "RoleSessionName": "airflow-{{ ti.dag_id }}",
#     "DurationSeconds": 3600
#   }
# }

dag = DAG(
    dag_id="list_s3_buckets_with_conn",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["s3", "example"],
)



# List all S3 buckets using the connection (with assume role if configured)
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


# Create a test bucket in the assumed role's account
def create_bucket_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    region = getattr(client.meta, "region_name", None)
    try:
        if region and region != "us-east-1":
            client.create_bucket(Bucket=test_bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            client.create_bucket(Bucket=test_bucket_name)
        print(f"Created bucket: {test_bucket_name} in region {region}")
    except client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {test_bucket_name} already owned by you")


create_bucket = PythonOperator(
    task_id="create_test_bucket_task",
    python_callable=create_bucket_fn,
    dag=dag,
)


# Write an object to the test bucket
def put_object_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    client.put_object(Bucket=test_bucket_name, Key="test.txt", Body="This is a test file.")
    print(f"Wrote test object to {test_bucket_name}/test.txt")


put_object = PythonOperator(
    task_id="put_object_in_test_bucket_task",
    python_callable=put_object_fn,
    dag=dag,
)


def delete_bucket_fn(**context):
    hook = S3Hook(aws_conn_id=default_conn_id)
    client = hook.get_conn()
    # delete all objects first
    try:
        objs = client.list_objects_v2(Bucket=test_bucket_name)
        if objs.get("KeyCount", 0) > 0:
            to_delete = {"Objects": [{"Key": o["Key"]} for o in objs.get("Contents", [])]}
            client.delete_objects(Bucket=test_bucket_name, Delete=to_delete)
        client.delete_bucket(Bucket=test_bucket_name)
        print(f"Deleted bucket: {test_bucket_name}")
    except botocore.exceptions.ClientError as e:
        print(f"Error deleting bucket {test_bucket_name}: {e}")


delete_bucket = PythonOperator(
    task_id="delete_test_bucket_task",
    python_callable=delete_bucket_fn,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

list_buckets >> create_bucket >> put_object >> delete_bucket
