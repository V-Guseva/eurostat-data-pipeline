import hashlib
import io
import json
import logging
from typing import Dict, Any

import pandas as pd
import pyarrow.parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from dags.source.eurostat.transform_rules import clean_eurostat_catalog
from utils.hash_utils import to_stable_data_hash
from utils.parquet_utils import write_parquet_bytes
from utils.path_layout import silver_key, bronze_key
from utils.storage import put_object, get_body

logger = logging.getLogger(__name__)

def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i: i + size]

class EurostatSource:
    def extract(self, df: pd.DataFrame, ds: str, dataset_code: str, run_id:int) -> Dict[str, Any]:
        csv_bytes = ((df.sort_values(by = df.columns.tolist())
                     .to_csv(index=False,lineterminator='\n'))
                     .encode('utf-8'))
        # resilience check
        sha = hashlib.sha256(csv_bytes).hexdigest()
        #form key and get buckets
        raw_bucket, raw_key = bronze_key("eurostat", dataset_code, ds, run_id, "raw.csv")
        meta_bucket,meta_key = bronze_key("eurostat", dataset_code, ds, run_id, "meta.json")
        logger.info(f"Raw bucket {raw_bucket}")
        put_object(raw_key, csv_bytes, content_type="text/csv", bucket_name=raw_bucket)
        put_object(meta_key, json.dumps({
            "sha256_raw": sha,
            "bytes": len(csv_bytes),
            "rows": int(len(df)),
            "schema_version": "1.0",
            "source": "eurostat",
            "dataset_code": dataset_code,
            "partition": f"daily={ds}",
            "artifact": {"bucket": raw_bucket, "key": raw_key}}).encode("utf-8"), content_type="application/json", bucket_name=meta_bucket)

        return {
            "source": "eurostat",
            "dataset_code": dataset_code,
            "partition": f"daily={ds}",
            "bronze_bucket": raw_bucket,
            "bronze_key": raw_key,
            "run_id": run_id,
            "meta_bucket": meta_bucket,
            "meta_key": meta_key
        }


    def transform(self,extract_result: dict) -> dict:
        source = extract_result["source"]
        dataset = extract_result["dataset_code"]
        ds = extract_result["partition"].split("=")[1]
        raw_bucket = extract_result["bronze_bucket"]
        raw_key = extract_result["bronze_key"]
        run_id = extract_result["run_id"]

        obj = get_body(key=raw_key, bucket_name=raw_bucket)
        df = pd.read_csv(io.BytesIO(obj))
        df = clean_eurostat_catalog(df)

        sha_data, data_bytes_len = to_stable_data_hash(df)

        pq_bytes = write_parquet_bytes(df)

        bucket, pq_key = silver_key(source, dataset, ds, run_id, "part-000.parquet")
        meta_bucket, meta_key = silver_key(source, dataset, ds, run_id, "meta.json")

        put_object(pq_key, pq_bytes, content_type="application/octet-stream",bucket_name=bucket)
        put_object(
            meta_key,
            json.dumps({
                "rows": int(len(df)),
                "columns": list(df.columns),
                "sha256_data": sha_data,
                "parquet_bytes": len(pq_bytes),
                "source_raw": raw_key
            }).encode("utf-8"),
            content_type="application/json",
            bucket_name=meta_bucket
        )

        return {
            **extract_result,
            "silver_key": pq_key,
            "silver_bucket": bucket,
            "silver_meta_key": meta_key,
            "rows": len(df),
            "sha256_data": sha_data,
            "format": "parquet",
        }

    def load(self, transform_result: dict, batch_size : int = 1000) -> dict:
        PG_CONN_ID = "pg_dn"

        bucket = transform_result["silver_bucket"]
        key    = transform_result["silver_key"]
        run_id = transform_result["run_id"]
        #Load parquet
        data = get_body(key, bucket_name=bucket)
        df = pq.read_table(io.BytesIO(data)).to_pandas()
        df = df[['code', 'title', 'last_update_timestamp', 'last_structure_change_timestamp',
                 'start_year', 'end_year', 'period']]
        cols = list(df.columns)
        non_keys = [c for c in cols if c not in ["code"]]
        #Form sql query
        col_list = ", ".join(f'"{c}"' for c in cols)
        val_placeholders = ", ".join(f":{c}" for c in cols)
        TARGET_TABLE = 'staging."eurostat_ds"'
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_keys)
        row_left  = "ROW(" + ", ".join(f'{TARGET_TABLE}."{c}"' for c in non_keys) + ")"
        row_right = "ROW(" + ", ".join(f'EXCLUDED."{c}"' for c in non_keys) + ")"
        # Update or add if changed
        upsert_sql = text(f"""
            INSERT INTO {TARGET_TABLE} ({col_list})
            VALUES ({val_placeholders})
            ON CONFLICT ("code")
            DO UPDATE SET {set_clause}
            WHERE {row_left} IS DISTINCT FROM {row_right};
        """)
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        total_rows = int(len(df))
        inserted = 0

        try:
            with pg.get_sqlalchemy_engine().begin() as conn:
                # TODO
                # conn.execute(text('CREATE SCHEMA IF NOT EXISTS staging;'))
                # conn.execute(text(f'CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ...

                for batch in _chunks(df.to_dict(orient="records"), batch_size):
                    res = conn.execute(upsert_sql, batch)
                    inserted += res.rowcount or 0

        except SQLAlchemyError as e:
            logger.error(e)
            raise

        return {
            **transform_result,
            "target_table": TARGET_TABLE,
            "total_rows": total_rows,
            "affected": inserted,
        }

