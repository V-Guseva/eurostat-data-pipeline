FROM apache/airflow:3.0.3
LABEL authors="v"
USER airflow
ENV AIRFLOW_UID=1000
COPY ../requirements.txt /opt/airflow/req_copy.txt
RUN pip install --no-cache-dir -r /opt/airflow/req_copy.txt
#RUN if [-d /opt/airflow/dags];then \
#      echo "Dags folder was removed." \
#      rm -r /opt/airflow/dags; \
#    fi
# COPY ../src/dags /opt/airflow/dags