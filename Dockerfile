# Pinned by digest: a tag can move, a digest cannot.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Dependencies first (cached layer), from the lock file, then the package itself.
COPY requirements.lock ./
RUN pip install -r requirements.lock
COPY pyproject.toml alembic.ini ./
COPY src ./src
COPY database ./database
RUN pip install --no-deps .

RUN useradd --uid 10001 --no-create-home fleetforge
USER fleetforge
EXPOSE 8000
CMD ["uvicorn", "fleetforge.api.app:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
