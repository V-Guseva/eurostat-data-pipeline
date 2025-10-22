from io import StringIO

import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI

from app.routers.eurostat_ds import router_eu_ds
from app.routers.health import router



app = FastAPI()
app.include_router(router)
app.include_router(router_eu_ds)



@app.get("/")
async def root():
    return {"message": "Eurostat API is running"}

@app.get("/ping")
async def ping():
    return {"ping": "pong"}


@app.get("/run-pipeline/all")
async def run_pipelines():
    result = None #run_all()
    return {"status": "success", "result": result}

@app.get("/run-pipeline/{code}")
async def run_pipeline(code: str):
    if 'catalog' in code:
        try:
            return {'status': "susses", 'message': "Test message"} #run_catalog_pipeline()}
        except Exception as e:
            return {'status': "error", 'message': str(e)}
    return None

@app.get("/run-pipeline/oecd")
async def run_pipelile_oecd():
    url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/.M.LI...AA...H?startPeriod=2023-02&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
    # Fetch data
    response = requests.get(url)
    # Load into pandas DataFrame
    df = pd.read_csv(StringIO(response.text))
    # Display first few rows
    print(df.head())
    return df



if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)