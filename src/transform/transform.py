import pandas as pd

from utils.helper import split_domain, convert_to_datetime


def clean_catalog(df:pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = df.dropna(subset=['code','title'])
    df = df[~df['code'].str.contains('\\$DV', na = False)]
    ## split data
    df[['start_year','start_period']] = df['data start'].str.split('-',n=1,expand=True)
    df[['end_year','end_period']] = df['data end'].str.split('-',n=1,expand=True)
    df['domain'] = df['code'].apply(split_domain)
    ## set data types
    df['start_year'] = pd.to_numeric(df['start_year'], errors='coerce')
    df['end_year'] = pd.to_numeric(df['end_year'], errors='coerce')
    df['last table structure change'] = convert_to_datetime(df['last table structure change'])
    df['last update of data'] = convert_to_datetime(df['last update of data'])
    df['title'] = df['title'].astype('str')
    return df


