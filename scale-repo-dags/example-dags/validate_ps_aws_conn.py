"""
Validation script to check Airflow connection 'ps-aws-config' extras for assume-role access.

This can be run inside an Airflow worker (PythonOperator) or locally where Airflow is installed.
It uses `AwsBaseHook` to create a boto3 session using the named connection and then calls STS and S3 operations.
"""

from __future__ import annotations

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import botocore

CONN_ID = "aws-assumerole-test"
TARGET_REGION = "us-east-2"


def validate_connection():
    hook = AwsBaseHook(aws_conn_id=CONN_ID, region_name=TARGET_REGION)
    session = hook.get_session()
    sts = session.client("sts", region_name=TARGET_REGION)
    s3 = session.client("s3", region_name=TARGET_REGION)

    try:
        caller = sts.get_caller_identity()
        print("STS get-caller-identity:", caller)
    except botocore.exceptions.ClientError as e:
        print("Failed STS call:", e)
        raise

    try:
        buckets = s3.list_buckets()
        print("S3 buckets (account):", [b["Name"] for b in buckets.get("Buckets", [])])
    except botocore.exceptions.ClientError as e:
        print("Failed S3 list_buckets:", e)
        raise


if __name__ == "__main__":
    validate_connection()

# Usage:
# - To run locally (where Airflow libs are installed):
#     python validate_ps_aws_conn.py
# - To run inside Airflow as a PythonOperator, import `validate_connection` and call it from the operator.
#
# Notes:
# - Ensure an Airflow connection named 'ps-aws-config' exists and its 'Extra' JSON contains the assume-role fields shown in the DAG file.
# - This script will print the assumed identity and list S3 buckets visible to that assumed role in the target region.
