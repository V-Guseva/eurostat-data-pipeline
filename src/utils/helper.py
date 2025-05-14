import re
import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame


def split_domain(code):
    match = re.match(r"^([A-Z]+)",code)
    if match:
        return match.group(1)
    return code

def convert_to_datetime(series):
    cleaned = series.astype('str').str.replace(r'([+-]\d{2}):(\d{2})', r'\1:\2', regex=True)
    return pd.to_datetime(cleaned, errors='coerce', utc=True)

def convert_object_to_category(df:DataFrame, threshold :int=5):
    description = df[df.select_dtypes('object')].describe()
    unique_counts = description.loc['unique']
    for column in unique_counts[unique_counts < threshold].index.tolist():
        df[column] = df[column].astype('category')
    return df