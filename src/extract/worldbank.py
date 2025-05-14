import requests
import pandas as pd
from io import StringIO

def load():
    # Define API query URL (CSV with labels format)
    url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/.M.LI...AA...H?startPeriod=2023-02&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
    # Fetch data
    response = requests.get(url)
    # Load into pandas DataFrame
    df = pd.read_csv(StringIO(response.text))
    # Display first few rows
    print(df.head())
    return df



if __name__ == "__main__":
    load()

