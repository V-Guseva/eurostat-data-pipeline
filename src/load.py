import pandas as pd


def save_catalog(df:pd.DataFrame, path:str ):
    df.to_csv(path, index=False)