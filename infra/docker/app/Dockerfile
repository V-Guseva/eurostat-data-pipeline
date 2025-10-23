FROM python:3.13-alpine  AS app
# python:alpine3.21 = Python 3.14.0
ENV PYTHONDONTWRITEBITYCODE=1 \
    PYTHONUNBUFFERED=1
COPY /src/app/requirements.txt /temp/requirements.txt
RUN pip install update
RUN pip install --no-cache-dir -r /temp/requirements.txt
EXPOSE 8000