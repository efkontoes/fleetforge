"""Lakehouse settings (environment prefix FLEETFORGE_LAKEHOUSE_).

Kept apart from fleetforge.config so the lakehouse never changes Milestone 2
configuration. Kafka and registry addresses still come from fleetforge.config.
Defaults match infrastructure/lakehouse/compose.yaml inside its network.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class LakehouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLEETFORGE_LAKEHOUSE_", extra="ignore")

    catalog_jdbc_url: str = "jdbc:postgresql://catalog-db:5432/iceberg_catalog"
    catalog_user: str = "iceberg"
    catalog_password: str = ""
    warehouse: str = "s3://lakehouse/warehouse"
    s3_endpoint: str = "http://garage:3900"
    s3_region: str = "us-east-1"
    s3_access_key: str = ""
    s3_secret_key: str = ""

    spark_master: str = "local[2]"
    driver_memory: str = "768m"  # JVM heap only; measured sufficient for slice 1
    # Off-heap ceilings. Without them the JVM grew to 1.35 GB RSS with a 768 MB heap
    # and was OOM-killed by its container (measured; see the spec's test log).
    driver_java_options: str = (
        "-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=320m -XX:ReservedCodeCacheSize=96m "
        "-XX:ParallelGCThreads=2 -XX:ConcGCThreads=1 -XX:CICompilerCount=2"
    )
    shuffle_partitions: int = 4
    jars_dir: str = ""  # baked jars (image); empty -> resolve SPARK_PACKAGES with Ivy

    @property
    def catalog_dsn(self) -> str:
        """libpq DSN of the catalog database, for the single-writer lease."""
        rest = self.catalog_jdbc_url.removeprefix("jdbc:postgresql://")
        hostport, _, db = rest.partition("/")
        host, _, port = hostport.partition(":")
        return (
            f"host={host} port={port or 5432} dbname={db} user={self.catalog_user} "
            f"password={self.catalog_password}"
        )
