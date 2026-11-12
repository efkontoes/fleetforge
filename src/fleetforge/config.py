from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLEETFORGE_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://fleetforge:fleetforge@localhost:5432/fleetforge"
    db_pool_size: int = 10
    db_max_overflow: int = 10
    db_pool_timeout_s: float = 5.0
    # How far observed_at may be ahead of the server clock before we call it implausible.
    max_clock_skew_s: float = 300.0
    # A latest state older than this is reported as stale by the read API.
    state_freshness_s: float = 10.0
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
