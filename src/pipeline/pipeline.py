import os
from pathlib import Path

from extract.eurostat import load_df_raw_catalog
from load import save_catalog
from transform.transform import clean_catalog

# General service
def run_all():
    print("Running all pipelines")
    return None

def check_is_loaded(code):
    data_dir = Path().resolve().parent / "data" / "processed"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_path = data_dir / f"{code}.csv"
    if save_path.is_file():
        return True, save_path
    return False, save_path

def run_catalog_pipeline() -> str:
    code = "eurostat_data_sets"
    is_loaded,path = check_is_loaded(code)
    if is_loaded:
        return f"Dataset '{code}' is already loaded"
    df = load_df_raw_catalog()
    df_clean = clean_catalog(df)
    save_catalog(df_clean,path)
    print("Catalog pipeline cleaned and saved")
    return f"Dataset '{code}' is now cleaned and saved"
