#!/bin/bash
export PYTHONPATH=src
uvicorn app.api_main:app --reload