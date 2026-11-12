"""Alembic environment. Migrations are hand-written SQL; there is no ORM autogenerate."""

from alembic import context
from sqlalchemy import create_engine, pool

from fleetforge.config import get_settings

url = context.config.attributes.get("database_url") or get_settings().database_url


def run_migrations_online() -> None:
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, transactional_ddl=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    context.configure(url=url, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    run_migrations_online()
