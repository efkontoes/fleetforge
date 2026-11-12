from sqlalchemy import Engine, create_engine

from fleetforge.config import Settings


def make_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_s,
        # Detect connections killed by a database restart before handing them out.
        pool_pre_ping=True,
        # Explicit, even though it is PostgreSQL's default: every correctness argument
        # in this codebase (conditional upsert, row locks) is made for READ COMMITTED.
        isolation_level="READ COMMITTED",
        connect_args={"connect_timeout": 3},
    )
