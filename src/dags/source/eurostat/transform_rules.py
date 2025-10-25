import re
import pandas as pd

from utils.transform_shared import convert_to_datetime, get_period


def clean_eurostat_catalog(df:pd.DataFrame) -> pd.DataFrame:
    """
    Do cleaning up dataset catalog
    :param df: catalog
    :return: pd.DataFrame formatted and cleaned
    """
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


