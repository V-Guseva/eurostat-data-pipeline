import io
from datetime import datetime, timedelta

from airflow.sdk import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin

from dags.helper import _connect_s3
from extract.worldbank import load_exp_gdp

log = LoggingMixin().log


@task
def extract():
    df = load_exp_gdp()
    s3 = _connect_s3("raw")
    bytes_buffer = io.BytesIO()
    df.to_parquet(path=bytes_buffer, engine="pyarrow")
    bytes_buffer.seek(0)
    s3.Bucket("raw").upload_fileobj(Fileobj=bytes_buffer, Key="test")
    return "test"


@task
def transform(key):
    log.info("Transforming data from {key}")
    return "test"


@task
def load(key):
    log.info("Loading data from {key}")
    return "test"


@dag(dag_id="load_wb_data",
     default_args={'start_date': datetime(2020, 1, 1), 'retries': 0, 'retry_delay': timedelta(minutes=5)})
def etl():
    key = extract()
    key2 = transform(key)
    load(key2)


etl()
