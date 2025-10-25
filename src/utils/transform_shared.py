import re
import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame


def convert_to_datetime(series):
    cleaned = series.astype('str').str.replace(r'([+-]\d{2}):(\d{2})', r'\1:\2', regex=True)
    return pd.to_datetime(cleaned, errors='coerce', utc=True)

def convert_object_to_category(df:DataFrame, threshold :int=5):
    description = df[df.select_dtypes('object')].describe()
    unique_counts = description.loc['unique']
    for column in unique_counts[unique_counts < threshold].index.tolist():
        df[column] = df[column].astype('category')
    return df

def convert_to_datetime(series):
    cleaned = series.astype('str').str.replace(r'([+-]\d{2}):(\d{2})', r'\1:\2', regex=True)
    return pd.to_datetime(cleaned, errors='coerce', utc=True)

def get_period(code):
    match = re.match(r"^([A-Z]+?)([AQ])+(_)+(.*)",code)
    if match:
        return  match.group(2)
    match2 = re.match(r"^([A-Z_0-9])+(_)+([AQ])",code)
    if match2:
        return match2.group(3)
    return None