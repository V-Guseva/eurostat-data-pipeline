import json
from typing import Dict, Any

import pandas as pd

from utils.hash_utils import to_stable_data_hash
from utils.parquet_utils import write_parquet_bytes
from utils.path_layout import bronze_key
from utils.storage import put_object


class WorldBankSource:
    def extract(self, df: pd.DataFrame, ds: str, dataset_code: str, run_id: int) -> Dict[str, Any]:
        # TODO temp
        source = "worldbank"
        dataset = "test"
        bucket, key = bronze_key(source, dataset, ds, run_id, "test.csv")
        meta_bucket, meta_key = bronze_key(source, dataset, ds, run_id, "meta.json")
        sha_data, data_bytes_len = to_stable_data_hash(df)
        pq_bytes = write_parquet_bytes(df)
        put_object(key, pq_bytes, "text/parquet", bucket)
        put_object(meta_key, json.dumps({
            "rows": int(len(df)),
            "columns": list(df.columns),
            "sha256_data": sha_data,
            "parquet_bytes": len(pq_bytes),
            "source_raw": key
        }).encode("utf-8"), "text/json", meta_bucket)
        return {
            "bronze_key": key,
            "bronze_bucket": bucket,
            "bronze_meta_key": meta_key,
            "rows": len(df),
            "sha256_data": sha_data,
            "format": "parquet",
        }