import pandas as pd
import pyarrow.parquet as pq
from pandas._libs.lib import pa


def write_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = pa.BufferOutputStream()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        use_deprecated_int96_timestamps=False,
        coerce_timestamps="ms",
    )
    return buf.getvalue().to_pybytes()