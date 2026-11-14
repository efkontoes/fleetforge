# Model training and drift checks on exported releases. Build context: repository root.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true
COPY requirements-ml.lock /tmp/
RUN pip install -r /tmp/requirements-ml.lock
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps . && useradd --uid 10001 --create-home fleetforge
USER fleetforge
ENTRYPOINT ["python", "-m"]
CMD ["fleetforge.ml.train", "--help"]
