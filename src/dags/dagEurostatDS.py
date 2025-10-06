# Import the libraries
# The DAG object; we'll need this to instantiate a DAG
import os
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import sqlalchemy as sa
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task_group
from airflow.sdk.definitions.decorators import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from dags.Helper import __connect_s3, __getStorageOptions
from extract import load_df_raw_catalog
from transform import clean_eurostat_catalog

log = LoggingMixin().log


@task_group(group_id="extract")
def extract():
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
        # TODO make it env variable
        busket_name = "raw"
        object_key = f"df_catalog_{time}.csv"
        s3 = __connect_s3(busket_name)
        try:
            req = s3.Bucket(busket_name).upload_file(tmp_file, object_key)
            log.info(f"Data frame {object_key} uploaded to {busket_name} req:{req}")
        except ClientError as e:
            log.error(e)
            raise
        log.info(f"File {object_key} uploaded to {busket_name}")
        return object_key

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def clear_tmp(extraction_result):
        tmp_file, time = extraction_result
        os.remove(tmp_file)
        log.info(f"Deleted {tmp_file}")

    extraction_result = extract_eurostat_data()
    object_key = save_to_s3(extraction_result)
    clear = clear_tmp(extraction_result)
    object_key >> clear
    return object_key


@task_group(group_id="transform")
def transform(object_s3_key: str):
    @task
    def transform_clean(object_s3_key):
        # TODO create constant
        raw_bucket = "raw"
        bucket = "staging"
        s3 = __connect_s3(raw_bucket)
        log.info(f"Downloading {object_s3_key}")
        df = pd.read_csv(s3.Object(bucket_name=raw_bucket, key=object_s3_key).get()["Body"])
        df = clean_eurostat_catalog(df)
        object_s3_key = object_s3_key.split(".")[0] + ".parquet"
        df.to_parquet(f"s3://{bucket}/{object_s3_key}", engine="pyarrow", compression="snappy", index=False,
                      storage_options=__getStorageOptions())
        return object_s3_key

    return transform_clean(object_s3_key)


@task_group(group_id="load")
def load(object_s3_key):
    @task
    def load_parquet(object_s3_key):
        bucket = "staging"
        s3 = __connect_s3(bucket)
        log.info(f"Downloading {object_s3_key}")
        s3_uri = f"s3://{bucket}/{object_s3_key}"
        df = pd.read_parquet(s3_uri, storage_options=__getStorageOptions())
        postgresHook = PostgresHook(postgres_conn_id="pg_dn")
        engine = postgresHook.get_sqlalchemy_engine()
        log.info(f"SQLAlchemy engine {sa.__version__} file:{getattr(sa, "__file__")} hasengine:{getattr(sa, "engine")}")
        df = df[['code', 'title', 'last_update_timestamp', 'last_structure_change_timestamp', 'start_year', 'end_year',
                 'period']]
        key_cols = ["code"]
        cols = list(df.columns)  # e.g. ["code","title","last_update_timestamp",...]
        non_keys = [c for c in cols if c not in key_cols]
        col_list = ",".join(f'"{c}"' for c in cols)
        val_placeholders = ",".join(f":{c}" for c in cols)
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_keys)
        # row-wise distinct: update only if any non-key column changed
        row_left = "ROW(" + ", ".join(f'"staging"."eurostat_ds".{c}' for c in non_keys) + ")"
        row_right = "ROW(" + ", ".join(f'EXCLUDED."{c}"' for c in non_keys) + ")"
        sql = text(
            f"""INSERT INTO "staging"."eurostat_ds" ({col_list})
                VALUES ({val_placeholders})
                ON CONFLICT ("{key_cols[0]}") 
                DO UPDATE
                    SET {set_clause}WHERE {row_left} IS DISTINCT FROM {row_right};""")
        try:
            with engine.begin() as conn:  # engine.begin()
                result = conn.execute(sql, df.to_dict(orient="records"))
                log.info(f"Inserted {result.rowcount}")
        except SQLAlchemyError as e:
            log.error(e)
            raise

    return load_parquet(object_s3_key)


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
    description='ETL DAG to fetch all datasets from Eurostat',
    catchup=False,
)
def etl():
    object_key = extract()
    transformed_df = transform(object_key)
    load(transformed_df)


etl()
