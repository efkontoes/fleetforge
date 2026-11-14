# dbt Gold models and data tests, run against Trino. Build context: repository root.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    DBT_PROFILES_DIR=/dbt DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs
COPY requirements-dbt.lock /tmp/
RUN pip install -r /tmp/requirements-dbt.lock
COPY dbt /dbt
RUN useradd --uid 10001 --create-home fleetforge
USER fleetforge
WORKDIR /dbt
ENTRYPOINT ["dbt"]
CMD ["--help"]
