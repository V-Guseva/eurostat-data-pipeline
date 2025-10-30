import posixpath

from airflow.sdk import Variable

_LAYER_TO_BUCKET = {
    "landing":"landing",
    "bronze": "bronze",
    "silver": "silver"
}

def _get_bucket_name(layer: str) -> str:
    """
    Return the bucket name for the given layer.
    :param layer: Literal[_LAYER_TO_BUCKET]
    :return: bucket name
    """
    var_name = _LAYER_TO_BUCKET.get(layer)
    if not var_name:
        raise ValueError(f"Unknown data layer: {layer}")
    return Variable.get(var_name)

def _build_key(
    layer: str,
    source: str,
    dataset: str,
    ds: str,
    run_id: str,
    filename: str,
    granularity: str = "daily",
) -> tuple[str, str]:
    """
    Construct S3 bucket/key path for data layer.
    Returns:
        (bucket_name, key)
    Example:
        ("staging", "eurostat/catalog/silver/daily=2025-10-23/<run_id>/file.parquet")
    """
    bucket = _get_bucket_name(layer)
    key = posixpath.join(source, dataset, layer, f"{granularity}={ds}", run_id, filename)
    return bucket, key

def landing_key(source: str, dataset: str, ds: str, run_id: str, filename: str):
    """
    Construct S3 bucket/key path for data layer.
    :param source:
    :param dataset:
    :param ds:
    :param run_id:
    :param filename:
    :return: (bucket_name, key)
    """
    return _build_key("landing", source, dataset, ds, run_id, filename)

#TODO pytest
def bronze_key(source: str, dataset: str, ds: str, run_id: str, filename: str):
    """
    Construct S3 bucket/key path for data layer.
    :param source:
    :param dataset:
    :param ds:
    :param run_id:
    :param filename:
    :return: (bucket_name, key)
    """
    return _build_key("bronze", source, dataset, ds, run_id, filename)

#TODO pytest
def silver_key(source: str, dataset: str, ds: str, run_id: str, filename: str):
    """
    Construct S3 bucket/key path for data layer.
    :param source:
    :param dataset:
    :param ds:
    :param run_id:
    :param filename:
    :return: (bucket_name, key)
    """
    return _build_key("silver", source, dataset, ds, run_id, filename)

