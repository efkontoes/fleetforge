"""Provision login credentials for the least-privilege roles created by migration 0005,
plus the schema registry's own database. Idempotent; re-running rotates passwords.

Passwords come only from the environment (generated into the git-ignored .env by
`make env`). Nothing secret is stored in migrations or source control.

    python -m fleetforge.ops.provision          # needs FLEETFORGE_DATABASE_URL (owner)
"""

from __future__ import annotations

import os
import sys

import psycopg
from psycopg import sql

from fleetforge.config import get_settings

LOGIN_ROLES = {
    "fleetforge_app": "FLEETFORGE_APP_DB_PASSWORD",
    "fleetforge_projector": "FLEETFORGE_PROJECTOR_DB_PASSWORD",
    "fleetforge_mission_consumer": "FLEETFORGE_MISSION_CONSUMER_DB_PASSWORD",
    "fleetforge_cdc": "FLEETFORGE_CDC_DB_PASSWORD",
    "fleetforge_readonly": "FLEETFORGE_READONLY_DB_PASSWORD",
}
REGISTRY_ROLE, REGISTRY_DB, REGISTRY_PW = "apicurio", "apicurio", "APICURIO_DB_PASSWORD"


def _dsn() -> str:
    return get_settings().database_url.replace("postgresql+psycopg://", "postgresql://")


def provision(env: dict[str, str] | None = None) -> list[str]:
    env = dict(os.environ if env is None else env)
    done = []
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        for role, var in LOGIN_ROLES.items():
            if not env.get(var):
                done.append(f"skip {role}: {var} not set (role stays NOLOGIN)")
                continue
            conn.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(env[var])
                )
            )
            done.append(f"login enabled: {role}")
        if env.get(REGISTRY_PW):
            exists = conn.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (REGISTRY_ROLE,)
            ).fetchone()
            verb = "ALTER" if exists else "CREATE"
            conn.execute(
                sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD {} CONNECTION LIMIT 10").format(
                    sql.Identifier(REGISTRY_ROLE), sql.Literal(env[REGISTRY_PW])
                )
            )
            if not conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (REGISTRY_DB,)
            ).fetchone():
                conn.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(REGISTRY_DB), sql.Identifier(REGISTRY_ROLE)
                    )
                )
            done.append("schema registry database ready")
    return done


if __name__ == "__main__":
    for line in provision():
        print(line)
    sys.exit(0)
