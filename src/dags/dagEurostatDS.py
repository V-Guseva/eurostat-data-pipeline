import importlib
from datetime import timedelta

import eurostat
import pendulum
from airflow.providers.standard.operators.python import get_current_context
from airflow.sdk.definitions.decorators import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin

from dags.source.eurostat import EurostatSource
from utils.registry import REGISTRY, register_instance

log = LoggingMixin().log
DATA_SET_NAME = "catalog"

default_args = {
    'owner': 'VGuseva',
    'start_date': pendulum.datetime(2025, 10, 1, tz="UTC"),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# TODO add on failure callback
@dag(
    dag_id='eurostat-catalog',
    schedule="@daily",
    default_args=default_args,
    description='ETL DAG to fetch all datasets from Eurostat',
    catchup=False,
    max_active_runs=1
)
def load_eurostat_catalog():
    importlib.import_module("dags.source.eurostat")
    register_instance("eurostat", EurostatSource())

    @task
    def run_extract(run_id, ds)->dict:
        """
        Saves extracted data to S3 bucket.
        :param extraction_result: Tuple[str, str] file name and timestamp
        :return: s3 object key
        """
        df = eurostat.get_toc_df()
        source = REGISTRY["eurostat"]
        log.info(f"Extracting {DATA_SET_NAME} from {type(source)}")
        return source.extract(df, ds , DATA_SET_NAME,run_id)

    @task
    def run_transform(extraction_result:dict)->dict:
        from utils.registry import REGISTRY
        source = REGISTRY["eurostat"]
        return source.transform(extraction_result)

    @task
    def run_load(transform_result: dict) -> dict:
        from utils.registry import REGISTRY
        return REGISTRY["eurostat"].load(transform_result,batch_size=2000)

    ext = run_extract(run_id="{{ run_id }}", ds="{{ ds }}")
    trn = run_transform(ext)
    load = run_load(trn)
    ext>>trn>>load


load_eurostat_catalog()
