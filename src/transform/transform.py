import re
import pandas as pd

from utils.helper import convert_to_datetime

def get_period(code):
    match = re.match(r"^([A-Z]+?)([AQ])+(_)+(.*)",code)
    if match:
        return  match.group(2)
    match2 = re.match(r"^([A-Z_0-9])+(_)+([AQ])",code)
    if match2:
        return match2.group(3)
    return None

def clean_eurostat_catalog(df:pd.DataFrame) -> pd.DataFrame:
    # set types and rename
    df['title'] = df['title'].astype('str')
    df['code'] = df['code'].astype('str')
    df['last_update_timestamp'] = convert_to_datetime(df['last update of data'])
    df['last_structure_change_timestamp'] = convert_to_datetime(df['last table structure change'])
    df['start_year'] = pd.to_numeric(df['data start'], errors='coerce')
    df['end_year'] = pd.to_numeric(df['data end'], errors='coerce')
    df = df.drop(columns=['data start', 'data end', 'last update of data', 'last table structure change','type'])
    #remove derived data sets
    df = df[~df['code'].str.contains('\\$DV', na=False)]
    df['period'] = df["code"].apply(get_period)
    df["period"] = df.apply(lambda x: 'YTD' if pd.isna(x["start_year"]) else x["period"], axis=1)
    df["start_year"] = df.apply(lambda x: x["last_update_timestamp"].year, axis=1)
    df["end_year"] = df.apply(lambda x: x["last_update_timestamp"].year, axis=1)
    return df


