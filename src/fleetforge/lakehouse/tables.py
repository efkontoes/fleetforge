"""Exact lakehouse table specifications: the single source of truth.

Every table is declared once here. The same declaration renders
  * the Spark SQL DDL the jobs execute (``Table.create_statements``), and
  * docs/architecture/milestone-3-tables.md (``render_markdown``; a unit test fails
    if the committed document drifts from this module).

Type conventions (Spark SQL names; Iceberg and Trino equivalents in the doc):
  * UUIDs are STRING in canonical lowercase hyphenated form. Iceberg has a uuid
    type, but Spark cannot write it reliably, and string keeps Spark and Trino in
    agreement.
  * TIMESTAMP is an instant (Iceberg ``timestamptz``, Trino
    ``timestamp(6) with time zone``), microsecond precision, stored as UTC.
  * NOT NULL is enforced by Iceberg on write. Identity columns (``identity``) are a
    logical key: Iceberg does NOT enforce uniqueness, so every writer must
    guarantee it, and tests check it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

CATALOG = "lake"  # Spark catalog name == Iceberg JDBC catalog_name == Trino catalog

COMMON_PROPERTIES: Mapping[str, str] = {
    # v2: row-level deletes. v3 exists, but Trino 484 calls its support experimental.
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    # Old metadata.json files are deleted after a commit, keeping the newest 50.
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # Spark SQL type
    nullable: bool
    doc: str


@dataclass(frozen=True)
class Table:
    namespace: str
    name: str
    doc: str
    columns: tuple[Column, ...]
    partitioning: tuple[str, ...] = ()  # Iceberg partition transforms
    sort_order: tuple[str, ...] = ()  # physical order within each data file
    identity: tuple[str, ...] = ()  # logical key, guaranteed by the writer
    properties: Mapping[str, str] = field(default_factory=dict)
    implemented: bool = True  # False: specified here, no job writes it yet

    @property
    def qualified(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.qualified} has no column {name}")

    def all_properties(self) -> dict[str, str]:
        return {**COMMON_PROPERTIES, **self.properties}

    def spark_schema(self) -> str:
        """DDL-style schema string (as accepted by mapInPandas / createDataFrame)."""
        return ", ".join(f"{c.name} {c.type}" for c in self.columns)

    def create_statements(self, catalog: str = CATALOG) -> list[str]:
        cols = ",\n  ".join(
            f"{c.name} {c.type}{'' if c.nullable else ' NOT NULL'} COMMENT '{c.doc}'"
            for c in self.columns
        )
        sql = f"CREATE TABLE IF NOT EXISTS {catalog}.{self.qualified} (\n  {cols}\n)\nUSING iceberg"
        if self.partitioning:
            sql += f"\nPARTITIONED BY ({', '.join(self.partitioning)})"
        sql += f"\nCOMMENT '{self.doc}'"
        props = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(self.all_properties().items()))
        sql += f"\nTBLPROPERTIES ({props})"
        out = [f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{self.namespace}", sql]
        if self.sort_order:
            # Iceberg SQL extension: stored in table metadata, applied by every writer.
            order = ", ".join(self.sort_order)
            out.append(f"ALTER TABLE {catalog}.{self.qualified} WRITE ORDERED BY {order}")
        return out


def C(name: str, type_: str, nullable: bool, doc: str) -> Column:  # noqa: N802 - table DSL
    return Column(name, type_, nullable, doc)


NULL, NOT_NULL = True, False

# Shared lineage columns: where a row came from in Kafka.
COORDINATES = (
    C("source_topic", "STRING", NOT_NULL, "Kafka topic name."),
    C("kafka_topic_id", "STRING", NOT_NULL,
      "Kafka topic id (incarnation). Changes when a topic is deleted and recreated."),
    C("kafka_partition", "INT", NOT_NULL, "Kafka partition."),
    C("kafka_offset", "BIGINT", NOT_NULL, "Kafka offset within the partition."),
)  # fmt: skip

# --------------------------------------------------------------------------- Bronze

BRONZE_KAFKA_RECORDS = Table(
    "bronze",
    "kafka_records",
    "Immutable copy of every ingested Kafka record, schema-agnostic. One row per "
    "(kafka_topic_id, kafka_partition, kafka_offset).",
    (
        *COORDINATES,
        C("kafka_timestamp", "TIMESTAMP", NULL,
          "Kafka record timestamp. FleetForge topics use CreateTime (producer clock). "
          "Transport metadata, never used for ordering."),
        C("kafka_timestamp_type", "STRING", NOT_NULL, "CREATE_TIME, LOG_APPEND_TIME or NONE."),
        C("record_key", "BINARY", NULL, "Exact key bytes (telemetry: UTF-8 robot_id)."),
        C("record_value", "BINARY", NULL, "Exact value bytes. NULL is a tombstone."),
        C("record_headers", "ARRAY<STRUCT<key: STRING, value: BINARY>>", NULL,
          "Kafka headers in record order (outbox topics carry eventType and outboxCreatedAt)."),
        C("value_sha256", "STRING", NULL, "Lowercase hex SHA-256 of record_value."),
        C("value_framing", "STRING", NOT_NULL,
          "CONFLUENT (magic byte 0 and a 4-byte schema id), NONE (any other bytes) or "
          "TOMBSTONE. Read from the bytes only: says nothing about whether the body decodes."),
        C("value_schema_id", "BIGINT", NULL,
          "Registry schema id from the framing (unsigned 32-bit); NULL unless CONFLUENT."),
        C("ingestion_run_id", "STRING", NOT_NULL, "Bronze ingestion run that wrote this row."),
        C("ingested_at", "TIMESTAMP", NOT_NULL,
          "Start of that run (lakehouse clock), constant within a run. Part of the "
          "deterministic arrival order."),
    ),
    partitioning=("source_topic", "days(ingested_at)"),
    sort_order=("kafka_partition", "kafka_offset"),
    identity=("kafka_topic_id", "kafka_partition", "kafka_offset"),
)  # fmt: skip

BRONZE_REGISTRY_SCHEMAS = Table(
    "bronze",
    "registry_schemas",
    "Archive of every schema id seen in Bronze, so history stays decodable even if the "
    "schema registry loses its storage. Insert-if-absent; an id never changes meaning.",
    (
        C("schema_id", "BIGINT", NOT_NULL, "Registry global schema id."),
        C("status", "STRING", NOT_NULL,
          "FOUND, or NOT_FOUND when the registry answered 404 while it demonstrably held "
          "the subject (an authoritative absence, never a transport failure)."),
        C("schema_type", "STRING", NULL, "AVRO for FOUND rows."),
        C("schema_text", "STRING", NULL, "Schema exactly as returned by the registry."),
        C("schema_sha256", "STRING", NULL, "Lowercase hex SHA-256 of schema_text."),
        C("registry_url", "STRING", NOT_NULL, "Registry the schema was fetched from."),
        C("first_seen_topic", "STRING", NOT_NULL, "Topic whose record referenced the id."),
        C("archived_at", "TIMESTAMP", NOT_NULL, "When the id was archived."),
        C("ingestion_run_id", "STRING", NOT_NULL, "Run that archived it."),
    ),
    identity=("schema_id",),
)  # fmt: skip

# --------------------------------------------------------------------------- Silver

SILVER_TELEMETRY_EVENTS = Table(
    "silver",
    "telemetry_events",
    "Validated canonical telemetry: one row per accepted event_id, canonical units. "
    "Ordered by (robot_id, boot_number, sequence_number), never by robot clock.",
    (
        C("event_id", "STRING", NOT_NULL, "Producer event identity (stable across redeliveries)."),
        C("robot_id", "STRING", NOT_NULL, "Robot."),
        C("boot_id", "STRING", NOT_NULL, "Boot session."),
        C("boot_number", "BIGINT", NOT_NULL,
          "Server-assigned boot ordinal. Copied from the event: the lakehouse cannot verify it "
          "against the boot registry (contract gap G2)."),
        C("sequence_number", "BIGINT", NOT_NULL, "Monotonic within a boot, starts at 0."),
        C("observed_at", "TIMESTAMP", NOT_NULL,
          "Robot clock. Used for partitioning and daily grains, never for ordering."),
        C("robot_model", "STRING", NOT_NULL, "From the RobotRegistered outbox event."),
        C("firmware_version", "STRING", NOT_NULL, "Firmware the event declares."),
        C("battery_pct", "DOUBLE", NOT_NULL, "Battery in percent [0, 100] after unit conversion."),
        C("x_m", "DOUBLE", NOT_NULL, "Position x in metres after unit conversion."),
        C("y_m", "DOUBLE", NOT_NULL, "Position y in metres after unit conversion."),
        C("heading_rad", "DOUBLE", NULL,
          "Heading wrapped into [-pi, pi). NULL for writer schema revision 1 or a "
          "non-finite source value (see quality_flags). Frame and zero direction are "
          "unspecified by the contract (gap G5)."),
        C("motor_temp_c", "DOUBLE", NULL, "Motor temperature; NULL if absent or non-finite."),
        C("lidar_ok", "BOOLEAN", NULL, "Lidar self-check; NULL if absent."),
        C("source_battery_unit", "STRING", NOT_NULL, "Firmware contract unit converted from."),
        C("source_position_unit", "STRING", NOT_NULL, "Firmware contract unit converted from."),
        C("quality_flags", "ARRAY<STRING>", NOT_NULL,
          "Field-level repairs and observations, sorted, e.g. HEADING_WRAPPED."),
        C("schema_version", "INT", NOT_NULL, "Business contract major version (1)."),
        C("writer_schema_id", "BIGINT", NOT_NULL, "Registry id of the writer schema."),
        C("producer_id", "STRING", NOT_NULL, "Producer identity (delivery metadata)."),
        C("produced_at", "TIMESTAMP", NOT_NULL, "Producer clock at encoding (delivery metadata)."),
        C("correction_of", "STRING", NULL, "event_id this correction supersedes."),
        C("superseded_by", "STRING", NULL, "Winning correction that replaced this event."),
        C("is_current", "BOOLEAN", NOT_NULL, "False when superseded_by is set."),
        C("arrived_out_of_order", "BOOLEAN", NOT_NULL,
          "An accepted event of the same robot with a greater (boot_number, sequence_number) "
          "arrived earlier."),
        C("duplicate_deliveries", "INT", NOT_NULL, "Later identical copies of this event."),
        *COORDINATES,
        C("kafka_timestamp", "TIMESTAMP", NULL, "Kafka timestamp of the accepted copy."),
        C("bronze_ingested_at", "TIMESTAMP", NOT_NULL, "Bronze ingested_at of the accepted copy."),
        C("silver_build_id", "STRING", NOT_NULL,
          "Hash of (Bronze snapshot, reference data, transform version): equal ids mean "
          "identical inputs and therefore identical rows."),
        C("transform_version", "STRING", NOT_NULL, "Silver rule set version."),
    ),
    partitioning=("days(observed_at)", "bucket(16, robot_id)"),
    sort_order=("robot_id", "boot_number", "sequence_number"),
    identity=("event_id",),
)  # fmt: skip

SILVER_TELEMETRY_RECORD_OUTCOMES = Table(
    "silver",
    "telemetry_record_outcomes",
    "Exactly one row per Bronze telemetry record: what Silver did with it and why. The "
    "quarantine is the REJECTED subset; the payload stays in Bronze (join on coordinates).",
    (
        *COORDINATES,
        C("outcome", "STRING", NOT_NULL, "ACCEPTED, DUPLICATE or REJECTED."),
        C("reason_code", "STRING", NULL, "Set for REJECTED only."),
        C("error_category", "STRING", NULL, "PERMANENT_DATA for REJECTED rows."),
        C("retry_eligible", "BOOLEAN", NOT_NULL,
          "Rejected only because reference data was missing; a rebuild after the reference "
          "data arrives can accept it."),
        C("errors", "STRING", NULL, "JSON array of {code, field, message, ...} details."),
        C("event_id", "STRING", NULL, "event_id if it could be read."),
        C("robot_id", "STRING", NULL, "robot_id if it could be read."),
        C("canonical_event_id", "STRING", NULL,
          "ACCEPTED: the event itself. DUPLICATE: the event_id whose first copy this repeats."),
        C("superseded_by", "STRING", NULL, "Correction that superseded the event of this record."),
        C("writer_schema_id", "BIGINT", NULL, "Schema id from the framing, if any."),
        C("bronze_ingested_at", "TIMESTAMP", NOT_NULL, "Bronze ingested_at."),
        C("silver_build_id", "STRING", NOT_NULL, "See silver.telemetry_events."),
        C("transform_version", "STRING", NOT_NULL, "Silver rule set version."),
    ),
    partitioning=("outcome", "days(bronze_ingested_at)"),
    sort_order=("kafka_partition", "kafka_offset"),
    identity=("kafka_topic_id", "kafka_partition", "kafka_offset"),
)  # fmt: skip

SILVER_FIRMWARE_CONTRACTS = Table(
    "silver",
    "firmware_contracts",
    "Firmware unit contracts as of the Bronze snapshot (latest CDC state per key).",
    (
        C("model_id", "STRING", NOT_NULL, "Robot model."),
        C("firmware_version", "STRING", NOT_NULL, "Firmware version."),
        C("battery_unit", "STRING", NOT_NULL, "PERCENT or FRACTION."),
        C("position_unit", "STRING", NOT_NULL, "M or MM."),
        C("cdc_op", "STRING", NOT_NULL, "Debezium op of the change that set this state."),
        *COORDINATES,
        C("silver_build_id", "STRING", NOT_NULL, "See silver.telemetry_events."),
    ),
    identity=("model_id", "firmware_version"),
)  # fmt: skip

SILVER_ROBOT_REGISTRY = Table(
    "silver",
    "robot_registry",
    "Robot models from RobotRegistered outbox events (robot model is immutable).",
    (
        C("robot_id", "STRING", NOT_NULL, "Robot."),
        C("robot_model", "STRING", NOT_NULL, "Model at registration."),
        C("registered_firmware_version", "STRING", NOT_NULL, "Firmware at registration."),
        C("registered_at", "STRING", NULL, "Registration time as published (ISO 8601 text)."),
        *COORDINATES,
        C("silver_build_id", "STRING", NOT_NULL, "See silver.telemetry_events."),
    ),
    identity=("robot_id",),
)  # fmt: skip

# --------------------------------------------------------------------------- Gold (specified)

GOLD_ROBOT_DAILY_TELEMETRY = Table(
    "gold",
    "robot_daily_telemetry",
    "Fleet telemetry analytics: one row per robot and observed day, current events only.",
    (
        C("observed_date", "DATE", NOT_NULL, "UTC date of observed_at."),
        C("robot_id", "STRING", NOT_NULL, "Robot."),
        C("robot_model", "STRING", NOT_NULL, "Robot model."),
        C("events", "BIGINT", NOT_NULL, "Current accepted events."),
        C("boots", "BIGINT", NOT_NULL, "Distinct boots with events that day."),
        C("expected_events", "BIGINT", NOT_NULL,
          "Sum over boots of (max - min sequence_number + 1) within the day."),
        C("missing_sequences", "BIGINT", NOT_NULL,
          "expected_events - events: lost, rejected or not yet arrived."),
        C("out_of_order_events", "BIGINT", NOT_NULL, "Events with arrived_out_of_order."),
        C("duplicate_deliveries", "BIGINT", NOT_NULL, "Sum of duplicate_deliveries."),
        C("corrected_events", "BIGINT", NOT_NULL, "Current events that are corrections."),
        C("battery_pct_min", "DOUBLE", NOT_NULL, "Minimum battery."),
        C("battery_pct_avg", "DOUBLE", NOT_NULL, "Mean battery over events."),
        C("battery_pct_max", "DOUBLE", NOT_NULL, "Maximum battery."),
        C("distance_m", "DOUBLE", NOT_NULL,
          "Sum of straight-line steps between consecutive sequence numbers of a boot; steps "
          "across a sequence gap are not counted."),
        C("motor_temp_c_max", "DOUBLE", NULL, "Maximum motor temperature."),
        C("lidar_fault_events", "BIGINT", NOT_NULL, "Events with lidar_ok = false."),
        C("first_observed_at", "TIMESTAMP", NOT_NULL, "Earliest observed_at."),
        C("last_observed_at", "TIMESTAMP", NOT_NULL, "Latest observed_at."),
        C("is_final", "BOOLEAN", NOT_NULL,
          "Day older than the allowed lateness (7 days, Kafka retention): late arrivals can "
          "no longer change the row."),
        C("silver_build_id", "STRING", NOT_NULL, "Silver build the row was computed from."),
    ),
    partitioning=("months(observed_date)",),
    identity=("observed_date", "robot_id"),
    implemented=False,
)  # fmt: skip

GOLD_PIPELINE_DAILY_QUALITY = Table(
    "gold",
    "pipeline_daily_quality",
    "Operational data quality: Silver outcomes per ingestion day, topic, outcome and reason.",
    (
        C("ingest_date", "DATE", NOT_NULL, "UTC date of bronze_ingested_at."),
        C("source_topic", "STRING", NOT_NULL, "Kafka topic."),
        C("outcome", "STRING", NOT_NULL, "ACCEPTED, DUPLICATE or REJECTED."),
        C("reason_code", "STRING", NOT_NULL,
          "Rejection reason, or NONE for ACCEPTED and DUPLICATE (key columns are never NULL)."),
        C("retry_eligible", "BOOLEAN", NOT_NULL, "See silver.telemetry_record_outcomes."),
        C("records", "BIGINT", NOT_NULL, "Bronze records."),
        C("distinct_events", "BIGINT", NOT_NULL, "Distinct non-NULL event_id."),
        C("distinct_robots", "BIGINT", NOT_NULL, "Distinct non-NULL robot_id."),
        C("silver_build_id", "STRING", NOT_NULL, "Silver build the row was computed from."),
    ),
    partitioning=("months(ingest_date)",),
    identity=("ingest_date", "source_topic", "outcome", "reason_code"),
    implemented=False,
)  # fmt: skip

GOLD_ROBOT_STATE_OBSERVATIONS = Table(
    "gold",
    "robot_state_observations",
    "Training-candidate robot state observations with stable ids, provenance and a "
    "leakage-safe split. Not training-ready: see the readiness requirements in the spec.",
    (
        C("release_id", "STRING", NOT_NULL, "Dataset release (gold.training_dataset_releases)."),
        C("observation_id", "STRING", NOT_NULL,
          "Silver event_id of the current version (the correction id when corrected)."),
        C("robot_id", "STRING", NOT_NULL, "Robot."),
        C("robot_model", "STRING", NOT_NULL, "Robot model."),
        C("firmware_version", "STRING", NOT_NULL, "Firmware."),
        C("episode_id", "STRING", NOT_NULL,
          "boot_id until mission-aligned episodes are specified (gap G7)."),
        C("boot_number", "BIGINT", NOT_NULL, "Boot ordinal."),
        C("sequence_number", "BIGINT", NOT_NULL, "Sequence within the boot."),
        C("step_index", "BIGINT", NOT_NULL,
          "sequence_number minus the first sequence_number of the episode in this release."),
        C("observed_at", "TIMESTAMP", NOT_NULL, "Robot clock."),
        C("battery_pct", "DOUBLE", NOT_NULL, "Canonical battery."),
        C("x_m", "DOUBLE", NOT_NULL, "Canonical x."),
        C("y_m", "DOUBLE", NOT_NULL, "Canonical y."),
        C("heading_rad", "DOUBLE", NULL, "Canonical heading."),
        C("motor_temp_c", "DOUBLE", NULL, "Motor temperature."),
        C("lidar_ok", "BOOLEAN", NULL, "Lidar self-check."),
        C("dt_prev_s", "DOUBLE", NULL,
          "Seconds since the previous sequence number of the same episode (observed_at); NULL "
          "at an episode start or after a sequence gap."),
        C("vx_mps", "DOUBLE", NULL, "Finite-difference x velocity when dt_prev_s > 0."),
        C("vy_mps", "DOUBLE", NULL, "Finite-difference y velocity when dt_prev_s > 0."),
        C("split", "STRING", NOT_NULL,
          "train, validation or test, by hash(episode_id, split_seed): no episode spans splits."),
        C("quality_flags", "ARRAY<STRING>", NOT_NULL, "Silver flags plus Gold flags."),
        *COORDINATES,
        C("silver_build_id", "STRING", NOT_NULL, "Silver build."),
    ),
    partitioning=("release_id", "split"),
    sort_order=("robot_id", "boot_number", "sequence_number"),
    identity=("release_id", "observation_id"),
    implemented=False,
)  # fmt: skip

GOLD_TRAINING_DATASET_RELEASES = Table(
    "gold",
    "training_dataset_releases",
    "One row per immutable dataset release: inputs, selection, split and content hash.",
    (
        C("release_id", "STRING", NOT_NULL, "Release identifier."),
        C("created_at", "TIMESTAMP", NOT_NULL, "Release time."),
        C("silver_build_id", "STRING", NOT_NULL, "Silver build used."),
        C("silver_events_snapshot_id", "BIGINT", NOT_NULL, "Iceberg snapshot read."),
        C("transform_version", "STRING", NOT_NULL, "Silver rule set version."),
        C("split_seed", "STRING", NOT_NULL, "Seed of the episode hash split."),
        C("selection", "STRING", NOT_NULL, "JSON of filters and exclusions applied."),
        C("rows_train", "BIGINT", NOT_NULL, "Rows in train."),
        C("rows_validation", "BIGINT", NOT_NULL, "Rows in validation."),
        C("rows_test", "BIGINT", NOT_NULL, "Rows in test."),
        C("content_sha256", "STRING", NOT_NULL,
          "Hash over the ordered rows: equal hashes mean an identical dataset."),
        C("readiness", "STRING", NOT_NULL,
          "CANDIDATE until the learning-readiness checks pass; SQL validation alone never "
          "promotes a release."),
        C("known_limitations", "STRING", NOT_NULL, "Free text, required."),
    ),
    identity=("release_id",),
    implemented=False,
)  # fmt: skip

ALL_TABLES: tuple[Table, ...] = (
    BRONZE_KAFKA_RECORDS,
    BRONZE_REGISTRY_SCHEMAS,
    SILVER_TELEMETRY_EVENTS,
    SILVER_TELEMETRY_RECORD_OUTCOMES,
    SILVER_FIRMWARE_CONTRACTS,
    SILVER_ROBOT_REGISTRY,
    GOLD_ROBOT_DAILY_TELEMETRY,
    GOLD_PIPELINE_DAILY_QUALITY,
    GOLD_ROBOT_STATE_OBSERVATIONS,
    GOLD_TRAINING_DATASET_RELEASES,
)

# Spark SQL type -> (Iceberg type, Trino type), for documentation.
_TYPE_MAP = {
    "STRING": ("string", "varchar"),
    "INT": ("int", "integer"),
    "BIGINT": ("long", "bigint"),
    "DOUBLE": ("double", "double"),
    "BOOLEAN": ("boolean", "boolean"),
    "DATE": ("date", "date"),
    "TIMESTAMP": ("timestamptz", "timestamp(6) with time zone"),
    "BINARY": ("binary", "varbinary"),
    "ARRAY<STRING>": ("list<string>", "array(varchar)"),
    "ARRAY<STRUCT<key: STRING, value: BINARY>>": (
        "list<struct<key: string, value: binary>>",
        "array(row(key varchar, value varbinary))",
    ),
}


def type_names(spark_type: str) -> tuple[str, str]:
    return _TYPE_MAP[spark_type]


def render_markdown(tables: Iterable[Table] = ALL_TABLES) -> str:
    out = [
        "# Milestone 3 — exact table schemas",
        "",
        "<!-- Generated by scripts/render_lakehouse_tables.py from",
        "     src/fleetforge/lakehouse/tables.py. Do not edit by hand. -->",
        "",
        "Catalog `lake` (Iceberg JDBC catalog; the same name in Spark and Trino).",
        "Common table properties: "
        + ", ".join(f"`{k}={v}`" for k, v in sorted(COMMON_PROPERTIES.items()))
        + ".",
        "",
        "Identity columns are a logical key. Iceberg does not enforce uniqueness: the "
        "writer guarantees it and tests check it.",
        "",
    ]
    for t in tables:
        status = "implemented (slice 1)" if t.implemented else "specified, not implemented yet"
        out += [f"## `{t.qualified}`", "", t.doc, "", f"* Status: {status}"]
        out.append(f"* Identity: `({', '.join(t.identity)})`" if t.identity else "* Identity: none")
        out.append(
            f"* Partitioning: `{', '.join(t.partitioning)}`" if t.partitioning else
            "* Partitioning: unpartitioned"
        )  # fmt: skip
        out.append(
            f"* Sort order: `{', '.join(t.sort_order)}`" if t.sort_order else "* Sort order: none"
        )
        extra = {k: v for k, v in t.properties.items()}
        if extra:
            out.append("* Extra properties: " + ", ".join(f"`{k}={v}`" for k, v in extra.items()))
        out += [
            "",
            "| Column | Spark | Iceberg | Trino | Null | Description |",
            "|---|---|---|---|---|---|",
        ]
        for c in t.columns:
            ice, trino = type_names(c.type)
            spark = c.type.replace("<", "&lt;").replace(">", "&gt;")
            ice = ice.replace("<", "&lt;").replace(">", "&gt;")
            null = "yes" if c.nullable else "no"
            out.append(f"| `{c.name}` | {spark} | {ice} | {trino} | {null} | {c.doc} |")
        out.append("")
    return "\n".join(out)
