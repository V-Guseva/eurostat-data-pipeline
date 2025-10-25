from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.providers.standard.operators.python import get_current_context
from airflow.sdk import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin


from dags.source.worldbank.loader import load_wb_test
from dags.source.worldbank.worldbank_source import WorldBankSource
from utils.registry import REGISTRY

log = LoggingMixin().log


@dag(dag_id="wb_data",
     default_args={'start_date': datetime(2025, 1, 1),
                   'retries': 2,
                   'retry_delay': timedelta(minutes=5)})
def etl():
    REGISTRY["worldbank"] = WorldBankSource()
    @task
    def extract(run_id,ds)->Dict[str, Any]:
        df = load_wb_test()
        source = REGISTRY["worldbank"]
        return source.extract(df,  ds, "test", run_id)

    @task
    def transform(extract_result:Dict[str,Any])->Dict[str,Any]:
        log.info(f"Transforming data from {extract_result["bronze_key"]}")
        return "test"

    @task
    def load(extract_result:Dict[str,Any])->Dict[str,Any]:
        log.info("Loading data from {key}")
        return "test"

    extract_result = extract(run_id="{{ run_id }}", ds="{{ ds }}")
    transform_result = transform(extract_result)
    load = load(transform_result)
    extract_result>>transform_result>>load


etl()
