# Airflow scheduler, DAG processor and API server. Jobs run as their own containers.
FROM apache/airflow:slim-3.3.2-python3.12@sha256:a54e7bd6c4d7a17e55cd14b0ead257877b8f8111680d2692cbdba2ff32bd3ff3
COPY infrastructure/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
COPY infrastructure/airflow/dags /opt/airflow/dags
COPY infrastructure/airflow/init_airflow.py /opt/airflow/init_airflow.py
# The DAGs share lakejobs.py; make it importable in every Airflow process.
ENV PYTHONPATH=/opt/airflow/dags
# A new auth volume copies this folder, and with it the airflow user as owner.
RUN mkdir -p /opt/airflow/auth
