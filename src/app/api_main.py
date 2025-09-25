from typing import Any

import uvicorn
from fastapi import FastAPI

import extract.worldbank
from pipeline.pipeline import run_all, run_catalog_pipeline

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Eurostat API is running"}


@app.get("/ping")
async def ping():
    return {"ping": "pong"}


@app.get("/run-pipeline/all")
async def run_pipelines():
    result = run_all()
    return {"status": "success", "result": result}

@app.get("/run-pipeline/{code}")
async def run_pipeline(code: str):
    if 'catalog' in code:
        try:
            return {'status': "susses", 'message': run_catalog_pipeline()}
        except Exception as e:
            return {'status': "error", 'message': str(e)}
    return None

@app.get("/run-pipeline/oecd")
async def run_pipelile_oecd():
    return extract.worldbank.load()



if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)