import os

import boto3
from botocore.exceptions import ClientError



def _connect_s3(busket_name):
    """
    Returns s3 connection after checking availability of minio and minio bucket.
    :param busket_name::
    :return: s3 connection
    """
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=os.getenv('MINIO_HOST'),
                            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
                            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'))
        try:
            s3.meta.client.head_bucket(Bucket=busket_name)
        except ClientError:
            log.error(f"Bucket {busket_name} does not found")
            raise
    except Exception as e:
        log.error("Unexpected error:", e)
        raise
    return s3


def _getStorageOptions():
    return {
        "key": os.getenv('MINIO_ACCESS_KEY'),
        "secret": os.getenv('MINIO_SECRET_KEY'),
        "client_kwargs": {"endpoint_url": os.getenv('MINIO_HOST')},
    }
