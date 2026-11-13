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
    # When false the legacy HTTP telemetry endpoint returns 410; telemetry must go
    # through Kafka. Kept on by default for Milestone 1 compatibility and benchmark A.
    http_telemetry_enabled: bool = True

    # ---- Kafka / schema registry (Milestone 2)
    kafka_bootstrap: str = "localhost:29092"
    schema_registry_url: str = "http://localhost:8081/apis/ccompat/v7"
    telemetry_topic: str = "telemetry.raw.v1"
    mission_events_topic: str = "fleet.mission-events.v1"

    # ---- projector
    projector_group: str = "telemetry-projector"
    projector_mode: str = "set"  # set | loop | per_event  (see projector/processor.py)
    projector_batch_size: int = 500
    projector_flush_interval_s: float = 0.2
    projector_metrics_port: int = 9101
    projector_max_retry_backoff_s: float = 10.0
    consumer_rejoin_timeout_s: float = 120.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
