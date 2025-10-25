import hashlib

import pandas as pd


def to_stable_data_hash(df: pd.DataFrame) -> str:
    csv_bytes = (
        df.sort_values(by=df.columns.tolist())
        .to_csv(index=False, lineterminator="\n")
        .encode("utf-8")
    )
    return hashlib.sha256(csv_bytes).hexdigest(), len(csv_bytes)
