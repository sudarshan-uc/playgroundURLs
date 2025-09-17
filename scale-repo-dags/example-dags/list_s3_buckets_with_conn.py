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

from airflow.providers.amazon.aws.operators.s3 import S3ListBucketsOperator, S3CreateBucketOperator, S3DeleteBucketOperator, S3PutObjectOperator
from airflow.utils.trigger_rule import TriggerRule
import uuid

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 9, 17)}



# Use an Airflow connection with assume role parameters in 'extra' for cross-account access
default_conn_id = "ps-aws-config"
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
list_buckets = S3ListBucketsOperator(
    task_id="list_s3_buckets_task",
    aws_conn_id=default_conn_id,
    dag=dag,
)


# Create a test bucket in the assumed role's account
create_bucket = S3CreateBucketOperator(
    task_id="create_test_bucket_task",
    bucket_name=test_bucket_name,
    aws_conn_id=default_conn_id,
    dag=dag,
)


# Write an object to the test bucket
put_object = S3PutObjectOperator(
    task_id="put_object_in_test_bucket_task",
    bucket_name=test_bucket_name,
    key="test.txt",
    data="This is a test file.",
    aws_conn_id=default_conn_id,
    dag=dag,
)


# Delete the test bucket (after object is written)
delete_bucket = S3DeleteBucketOperator(
    task_id="delete_test_bucket_task",
    bucket_name=test_bucket_name,
    aws_conn_id=default_conn_id,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

list_buckets >> create_bucket >> put_object >> delete_bucket
