# Import the libraries
# The DAG object; we'll need this to instantiate a DAG
import json
import os
from datetime import datetime, timedelta

import boto3
import pendulum
from airflow.sdk.definitions.decorators import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import EndpointConnectionError, ClientError
from extract import load_df_raw_catalog

log = LoggingMixin().log


@task
def extract_eurostat_data():
    df = load_df_raw_catalog()
    time = datetime.today().isoformat(timespec='seconds')
    tmp_file = f"/tmp/catalog_{time}.csv"
    df.to_csv(tmp_file)
    log.info(f"Extracted {tmp_file}")
    return tmp_file, time


@task
def save_to_s3(extraction_result):
    tmp_file, time = extraction_result
    basket_name = "raw"
    object_name = f"df_catalog_{time}.csv"
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=os.getenv('MINIO_HOST'),
                            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
                            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'))
        try:
            s3.meta.client.list_buckets()
        except EndpointConnectionError:
            log.error(f"Failed to connect to minio {os.getenv('MINIO_HOST')}")
            raise
        try:
            s3.meta.client.head_bucket(Bucket=basket_name)
        except ClientError:
            log.error(f"Bucket {basket_name} does not found")
            raise
        req = s3.Bucket(basket_name).upload_file(tmp_file, object_name)
        log.info(f"Data frame {object_name} uploaded to {basket_name} req:{req}")
    except Exception as e:
        log.error("Unexpected error:", e)
        raise
    log.info(f"File {object_name} uploaded to {basket_name}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def clear_tmp(extraction_result):
    tmp_file, time = extraction_result
    os.remove(tmp_file)
    log.info(f"Deleted {tmp_file}")


default_args = {
    'owner': 'VGuseva',
    'start_date': pendulum.today("UTC"),
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}


@dag(
    dag_id='eurostat-etl-dag',
    schedule="@daily",
    default_args=default_args,
    description='ETL DAG to fetch data from Eurostat',
    catchup=False,
)
def etl():
    extract_result = extract_eurostat_data()
    upload_task = save_to_s3(extract_result)
    clean_up_task = clear_tmp(extract_result)
    extract_result >> upload_task >> clean_up_task


etl()
