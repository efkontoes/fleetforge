"""One-shot Airflow setup: metadata database and role, schema migration, login, pool."""

import json
import os
import subprocess

import psycopg2

admin = psycopg2.connect(host="catalog-db", dbname="postgres", user="iceberg",
                         password=os.environ["AIRFLOW_VAR_LAKEHOUSE_CATALOG_PASSWORD"])  # fmt: skip
admin.autocommit = True
with admin.cursor() as cur:
    password = os.environ["AIRFLOW_DB_PASSWORD"]
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'airflow'")
    verb = "ALTER" if cur.fetchone() else "CREATE"
    cur.execute(f"{verb} ROLE airflow LOGIN PASSWORD %s", (password,))
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE airflow OWNER airflow")
admin.close()

subprocess.run(["airflow", "db", "migrate"], check=True)
with open(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"], "w") as f:
    json.dump({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, f)
subprocess.run(["airflow", "pools", "set", "lakehouse_writers", "1",
                "One lakehouse writer at a time"], check=True)  # fmt: skip
print("airflow initialised")
