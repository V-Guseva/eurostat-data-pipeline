import logging
from typing import Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def put_object(key: str, data: bytes, content_type: str, bucket_name: str):
    try:
        hook = S3Hook(aws_conn_id="minio", extra_args={"ContentType": content_type})
        hook.load_bytes(bytes_data=data, key=key, bucket_name=bucket_name, replace=False)
        logger.info(f"{key} uploaded to {bucket_name}")
    except ClientError as e:
        logger.exception(f"Failed to upload object {key} to bucket {bucket_name}. Error: {e}")
        raise e

def get_object(key: str, bucket_name: str):
    try:
        hook = S3Hook(aws_conn_id="minio")
        return hook.get_key(key, bucket_name=bucket_name)
    except ClientError as e:
        logger.exception(f"Failed to download object {key}. Error: {e}")
        raise e

def get_body(key: str, bucket_name: str) -> Union[bytes,str]:
    obj = get_object(key, bucket_name=bucket_name)
    return obj.get()["Body"].read()

