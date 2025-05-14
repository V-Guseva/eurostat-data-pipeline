Steps to set up env

1. Create virtual environment
uv venv .venv
2. Activate virtual environment
uv venv .venv
3. Install eurostat
uv pip install eurostat
4. Install eurostat
uv pip install notebook
5. Install fast api
uv pip install fastapi uvicorn
6. Activate
source .venv/bin/activate


To run jupiter notebook
jupyter notebook

To run fast api application
uvicorn app.main:app reload# eurostat-data-pipeline
