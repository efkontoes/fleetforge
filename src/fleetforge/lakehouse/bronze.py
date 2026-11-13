"""Bronze: an immutable, schema-agnostic copy of the Kafka log.

Identity of a Bronze row = (kafka_topic_id, kafka_partition, kafka_offset). The
topic *id* (not the name) is part of it because a deleted and recreated topic
restarts at offset 0 under the same name (the incident behind Milestone 2 ADR 0011).

How a replay is prevented from duplicating rows (two independent mechanisms):

1. Checkpoint. Each Bronze commit records, in the Iceberg snapshot summary, the next
   offset per (topic, partition). Data and checkpoint are one atomic Iceberg commit,
   the same idea as Milestone 2's offset ledger in the projector's transaction. The
   next run plans from that checkpoint (``plan_topic``), and fails closed on every
   disagreement with the Kafka log instead of guessing.
2. Insert-if-absent. Before appending, the job anti-joins the batch against Bronze
   rows in the same offset ranges (``range_predicate_sql`` keeps that read small).
   A deliberately or accidentally re-read range adds nothing.

Neither mechanism handles two concurrent writers. A single-writer lease does: see
docs/architecture/milestone-3.md (failure F10).

Everything here is pure; ``fleetforge.lakehouse.jobs.bronze_ingest`` runs it on Spark.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fleetforge.lakehouse.tables import BRONZE_KAFKA_RECORDS

CONFLUENT, NONE, TOMBSTONE = "CONFLUENT", "NONE", "TOMBSTONE"
# Kafka timestamp type as reported by librdkafka and Spark's Kafka source.
TIMESTAMP_TYPES = {0: "CREATE_TIME", 1: "LOG_APPEND_TIME"}

CHECKPOINT_PROPERTY = "fleetforge.bronze.checkpoint"
RUN_ID_PROPERTY = "fleetforge.bronze.run-id"
RANGES_PROPERTY = "fleetforge.bronze.ranges"
ACCEPTED_GAPS_PROPERTY = "fleetforge.bronze.accepted-gaps"

# Topic names (Kafka's legal alphabet) and topic ids (base64url UUIDs). Anything else
# is refused before it is ever placed into SQL.
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_TOPIC_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass(frozen=True)
class KafkaRecord:
    """One Kafka record, as any client returns it. Spark's Kafka source exposes the
    same fields as columns (topic, partition, offset, timestamp, timestampType, key,
    value, headers)."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None
    timestamp_type: int = 0
    headers: tuple[tuple[str, bytes | None], ...] = ()


def framing(value: bytes | None) -> tuple[str, int | None]:
    """Confluent wire format: magic byte 0, 4-byte big-endian schema id, Avro body.

    Same test as Milestone 2's TelemetryDecoder (fewer than 5 bytes, or a first byte
    other than 0, is not framed). Spark mirror: SPARK_FRAMING_SQL."""
    if value is None:
        return TOMBSTONE, None
    if len(value) >= 5 and value[0] == 0:
        return CONFLUENT, int.from_bytes(value[1:5], "big")
    return NONE, None


# Spark SQL expressions producing the derived Bronze columns from the Kafka source
# columns. Must agree with framing()/bronze_row(); tests/lakehouse checks they do.
_IS_FRAMED = "(length(value) >= 5 AND hex(substring(value, 1, 1)) = '00')"
SPARK_FRAMING_SQL = {
    "kafka_timestamp": "CASE WHEN unix_millis(timestamp) >= 0 THEN timestamp END",
    "value_sha256": "sha2(value, 256)",
    "value_framing": (
        f"CASE WHEN value IS NULL THEN '{TOMBSTONE}' WHEN {_IS_FRAMED} THEN '{CONFLUENT}' "
        f"ELSE '{NONE}' END"
    ),
    "value_schema_id": (
        f"CASE WHEN value IS NOT NULL AND {_IS_FRAMED} "
        "THEN CAST(conv(hex(substring(value, 2, 4)), 16, 10) AS BIGINT) END"
    ),
    "kafka_timestamp_type": (
        "CASE timestampType WHEN 0 THEN 'CREATE_TIME' WHEN 1 THEN 'LOG_APPEND_TIME' ELSE 'NONE' END"
    ),
}


def bronze_row(
    rec: KafkaRecord, *, topic_id: str, run_id: str, ingested_at: datetime
) -> dict[str, Any]:
    """The Bronze row for one Kafka record. Keys are exactly the table's columns."""
    kind, schema_id = framing(rec.value)
    ts = (
        datetime.fromtimestamp(rec.timestamp_ms / 1000, tz=UTC)
        if rec.timestamp_ms is not None and rec.timestamp_ms >= 0
        else None
    )
    row = {
        "source_topic": rec.topic,
        "kafka_topic_id": topic_id,
        "kafka_partition": rec.partition,
        "kafka_offset": rec.offset,
        "kafka_timestamp": ts,
        "kafka_timestamp_type": TIMESTAMP_TYPES.get(rec.timestamp_type, "NONE"),
        "record_key": rec.key,
        "record_value": rec.value,
        "record_headers": [{"key": k, "value": v} for k, v in rec.headers],
        "value_sha256": hashlib.sha256(rec.value).hexdigest() if rec.value is not None else None,
        "value_framing": kind,
        "value_schema_id": schema_id,
        "ingestion_run_id": run_id,
        "ingested_at": ingested_at,
    }
    return row


BRONZE_COLUMNS = BRONZE_KAFKA_RECORDS.column_names


def coordinates(row: Mapping[str, Any]) -> tuple[str, int, int]:
    return (row["kafka_topic_id"], row["kafka_partition"], row["kafka_offset"])


def arrival_key(row: Mapping[str, Any]) -> tuple:
    """Deterministic arrival order of Bronze rows: ingestion run start, then position
    in the log. Within one partition this is offset order (runs ingest offsets in
    increasing order). Across topic incarnations it is ingestion order. Stored
    values only, so every rebuild sees the same order."""
    return (row["ingested_at"], row["kafka_topic_id"], row["kafka_partition"], row["kafka_offset"])


def new_rows(existing: Iterable[tuple[str, int, int]], rows: Iterable[dict]) -> list[dict]:
    """Insert-if-absent, the reference for the job's anti-join: rows whose identity is
    not already in Bronze, first copy wins within the batch."""
    seen = set(existing)
    out = []
    for r in rows:
        k = coordinates(r)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


# --------------------------------------------------------------------------- checkpoints


@dataclass(frozen=True)
class TopicCheckpoint:
    topic: str
    topic_id: str
    next_offsets: Mapping[int, int]  # partition -> next offset to ingest


def encode_checkpoints(cps: Mapping[str, TopicCheckpoint]) -> str:
    body = {
        t: {"topic_id": cp.topic_id,
            "next_offsets": {str(p): o for p, o in sorted(cp.next_offsets.items())}}
        for t, cp in sorted(cps.items())
    }  # fmt: skip
    return json.dumps(body, sort_keys=True, separators=(",", ":"))


def decode_checkpoints(text: str | None) -> dict[str, TopicCheckpoint]:
    if not text:
        return {}
    body = json.loads(text)
    return {
        t: TopicCheckpoint(t, v["topic_id"], {int(p): int(o) for p, o in v["next_offsets"].items()})
        for t, v in body.items()
    }


def checkpoints_from_data(
    rows: Iterable[tuple[str, str, int, int, datetime]],
) -> dict[str, TopicCheckpoint]:
    """Fallback when no snapshot in the table's ancestry carries a checkpoint (for
    example after snapshot expiry): derive it from the data.

    rows: (topic, topic_id, partition, max_offset, last_ingested_at), one per
    (topic, topic_id, partition). The incarnation ingested most recently wins.
    Contiguity is assumed: max_offset + 1 is next only because runs never skip
    offsets without recording an accepted gap."""
    latest: dict[str, tuple[datetime, str]] = {}
    rows = list(rows)
    for topic, topic_id, _, _, at in rows:
        if topic not in latest or at > latest[topic][0]:
            latest[topic] = (at, topic_id)
    out: dict[str, dict[int, int]] = {}
    for topic, topic_id, part, max_offset, _ in rows:
        if latest[topic][1] == topic_id:
            out.setdefault(topic, {})[part] = max_offset + 1
    return {t: TopicCheckpoint(t, latest[t][1], parts) for t, parts in out.items()}


def merge_checkpoints(
    from_snapshots: Mapping[str, TopicCheckpoint], from_data: Mapping[str, TopicCheckpoint]
) -> dict[str, TopicCheckpoint]:
    """Combine the committed checkpoint with the one derived from the data.

    Normally they agree. The data can be ahead when a run found only rows that were
    already in Bronze (a replay) and so committed nothing; taking the larger offset
    keeps such a run from being repeated forever. When the topic ids differ, the
    snapshot wins: it records the newest operator decision (an accepted new
    incarnation that has no rows yet)."""
    out = dict(from_data)
    for topic, cp in from_snapshots.items():
        data = from_data.get(topic)
        if data is None or data.topic_id != cp.topic_id:
            out[topic] = cp
            continue
        parts = dict(data.next_offsets)
        for p, o in cp.next_offsets.items():
            parts[p] = max(o, parts.get(p, o))
        out[topic] = TopicCheckpoint(topic, cp.topic_id, parts)
    return out


def latest_checkpoint(
    snapshots: Iterable[tuple[int, int | None, Mapping[str, str]]], current: int | None
) -> dict[str, TopicCheckpoint]:
    """Walk from the current snapshot through its ancestors to the newest one that
    carries a checkpoint. Maintenance commits (compaction, expiry) have none.

    snapshots: (snapshot_id, parent_id, summary) for the snapshots still in metadata."""
    by_id = {sid: (parent, summary) for sid, parent, summary in snapshots}
    sid = current
    while sid is not None and sid in by_id:
        parent, summary = by_id[sid]
        if CHECKPOINT_PROPERTY in summary:
            return decode_checkpoints(summary[CHECKPOINT_PROPERTY])
        sid = parent
    return {}


# --------------------------------------------------------------------------- planning


class BronzePositionError(RuntimeError):
    """The checkpoint cannot be reconciled with the Kafka log. Never retried
    automatically: an operator decides (accept a gap, accept a new incarnation)."""


class BronzeReadError(RuntimeError):
    """The records read do not match the planned ranges. Nothing was committed; the
    run can be retried. Persisting failures are escalated."""


@dataclass(frozen=True)
class Watermarks:
    low: int  # earliest offset still in the log
    high: int  # log end offset: the next offset to be written


@dataclass(frozen=True)
class OffsetRange:
    topic: str
    topic_id: str
    partition: int
    start: int  # inclusive
    end: int  # exclusive

    @property
    def size(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class TopicPlan:
    topic: str
    topic_id: str
    ranges: tuple[OffsetRange, ...]
    next_checkpoint: TopicCheckpoint
    accepted_gaps: tuple[tuple[int, int, int], ...] = ()  # (partition, from, to)
    notes: tuple[str, ...] = field(default=())


def plan_topic(
    topic: str,
    topic_id: str,
    checkpoint: TopicCheckpoint | None,
    watermarks: Mapping[int, Watermarks],
    *,
    max_records_per_partition: int | None = None,
    accept_gaps: bool = False,
    accept_new_incarnation: bool = False,
) -> TopicPlan:
    """Offset ranges for one run, or BronzePositionError. Rules (cf. ADR 0011):

    * no checkpoint: first ingestion, start at the earliest retained offset.
    * checkpoint for another topic id: the topic was recreated. Refuse unless the
      operator accepts the new incarnation (which then starts like a new topic).
    * checkpoint below the low watermark: retention deleted records never ingested.
      Refuse unless the operator accepts the gap, which is recorded in the commit.
    * checkpoint above the high watermark: the log lost records it had served
      (truncation, unclean leader election). Always refuse.
    * a partition in the checkpoint that the topic no longer has: refuse.
    """
    _check_topic(topic, topic_id)
    notes: list[str] = []
    if checkpoint is not None and checkpoint.topic_id != topic_id:
        if not accept_new_incarnation:
            raise BronzePositionError(
                f"{topic}: checkpoint is for topic id {checkpoint.topic_id}, the topic is now "
                f"{topic_id} (deleted and recreated); accept the new incarnation explicitly"
            )
        notes.append(f"new incarnation {topic_id} accepted (previous {checkpoint.topic_id})")
        checkpoint = None
    if checkpoint is None:
        notes.append("first ingestion of this topic incarnation")
    else:
        gone = sorted(set(checkpoint.next_offsets) - set(watermarks))
        if gone:
            raise BronzePositionError(f"{topic}: partitions {gone} disappeared from the topic")

    ranges, gaps, nxt = [], [], {}
    for part in sorted(watermarks):
        wm = watermarks[part]
        if wm.low > wm.high or wm.low < 0:
            raise BronzePositionError(f"{topic}/{part}: invalid watermarks {wm}")
        start = checkpoint.next_offsets.get(part) if checkpoint else None
        if start is None:
            start = wm.low
            if checkpoint is not None:
                notes.append(f"new partition {part} starts at {wm.low}")
            elif wm.low > 0:
                notes.append(f"partition {part}: offsets [0, {wm.low}) predate ingestion")
        if start > wm.high:
            raise BronzePositionError(
                f"{topic}/{part}: checkpoint {start} is beyond the log end {wm.high}; the log "
                "lost records it had already served"
            )
        if start < wm.low:
            if not accept_gaps:
                raise BronzePositionError(
                    f"{topic}/{part}: records [{start}, {wm.low}) were deleted by retention "
                    "before ingestion; accept the gap explicitly"
                )
            gaps.append((part, start, wm.low))
            start = wm.low
        end = wm.high
        if max_records_per_partition is not None:
            end = min(end, start + max_records_per_partition)
        if end > start:
            ranges.append(OffsetRange(topic, topic_id, part, start, end))
        nxt[part] = end
    return TopicPlan(topic, topic_id, tuple(ranges), TopicCheckpoint(topic, topic_id, nxt),
                     tuple(gaps), tuple(notes))  # fmt: skip


def verify_batch(
    ranges: Iterable[OffsetRange],
    observed: Mapping[tuple[str, int], tuple[int, int, int]],
    *,
    contiguous: bool,
) -> None:
    """Check what was read against what was planned, before committing.

    observed: (topic, partition) -> (count, min_offset, max_offset) of the batch.
    Contiguous topics (written without Kafka transactions, no compaction) must
    deliver every offset; others (compacted topics, transactional producers) may
    have holes but nothing outside the range."""
    for r in ranges:
        got = observed.get((r.topic, r.partition))
        if got is None or got[0] == 0:
            if contiguous:
                raise BronzeReadError(f"{r.topic}/{r.partition}: planned {r.size} records, read 0")
            continue
        count, lo, hi = got
        if lo < r.start or hi >= r.end:
            raise BronzeReadError(
                f"{r.topic}/{r.partition}: read offsets [{lo}, {hi}] outside [{r.start}, {r.end})"
            )
        if contiguous and (count, lo, hi) != (r.size, r.start, r.end - 1):
            raise BronzeReadError(
                f"{r.topic}/{r.partition}: planned [{r.start}, {r.end}) = {r.size} records, read "
                f"{count} in [{lo}, {hi}]"
            )
    planned = {(r.topic, r.partition) for r in ranges}
    extra = sorted(set(observed) - planned)
    if any(observed[k][0] for k in extra):
        raise BronzeReadError(f"read unplanned partitions {extra}")


def range_predicate_sql(ranges: Iterable[OffsetRange]) -> str:
    """SQL predicate selecting the Bronze rows that could collide with a batch.

    Literal values let Iceberg prune: source_topic is a partition column, and files
    sorted by (kafka_partition, kafka_offset) carry tight min/max statistics, so
    only files overlapping the ranges are read by the anti-join."""
    terms, topics = [], set()
    for r in ranges:
        _check_topic(r.topic, r.topic_id)
        topics.add(r.topic)
        terms.append(
            f"(kafka_topic_id = '{r.topic_id}' AND kafka_partition = {int(r.partition)} "
            f"AND kafka_offset >= {int(r.start)} AND kafka_offset < {int(r.end)})"
        )
    if not terms:
        return "FALSE"
    topic_list = ", ".join(f"'{t}'" for t in sorted(topics))
    return f"source_topic IN ({topic_list}) AND ({' OR '.join(terms)})"


def spark_offsets_json(ranges: Iterable[OffsetRange]) -> tuple[str, str]:
    """startingOffsets / endingOffsets options for Spark's Kafka batch source."""
    start: dict[str, dict[str, int]] = {}
    end: dict[str, dict[str, int]] = {}
    for r in ranges:
        start.setdefault(r.topic, {})[str(r.partition)] = r.start
        end.setdefault(r.topic, {})[str(r.partition)] = r.end
    return json.dumps(start, sort_keys=True), json.dumps(end, sort_keys=True)


def snapshot_properties(
    run_id: str, plans: Iterable[TopicPlan], previous: Mapping[str, TopicCheckpoint]
) -> dict[str, str]:
    """Snapshot summary written atomically with the data. The checkpoint carries
    every topic (copied forward), so the newest ingestion snapshot is enough."""
    plans = list(plans)
    merged = dict(previous)
    for p in plans:
        merged[p.topic] = p.next_checkpoint
    props = {
        RUN_ID_PROPERTY: run_id,
        CHECKPOINT_PROPERTY: encode_checkpoints(merged),
        RANGES_PROPERTY: json.dumps(
            [[r.topic, r.topic_id, r.partition, r.start, r.end] for p in plans for r in p.ranges]
        ),
    }
    gaps = [[p.topic, p.topic_id, *g] for p in plans for g in p.accepted_gaps]
    if gaps:
        props[ACCEPTED_GAPS_PROPERTY] = json.dumps(gaps)
    return props


def _check_topic(topic: str, topic_id: str) -> None:
    if not _TOPIC_RE.match(topic):
        raise ValueError(f"illegal topic name {topic!r}")
    if not _TOPIC_ID_RE.match(topic_id):
        raise ValueError(f"illegal topic id {topic_id!r}")
