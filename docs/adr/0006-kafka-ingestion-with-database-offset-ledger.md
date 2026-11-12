# ADR 0006 — Kafka ingestion with a PostgreSQL offset ledger

Status: accepted (Milestone 2)

## Context
Milestone 1 accepted telemetry over HTTP with one transaction per event. We need
buffering (the database can be slow or down without robots losing data), replay,
and several independent consumers of the same events. Kafka offsets and
PostgreSQL commits cannot be committed atomically.

## Decision
* Robots publish Avro telemetry to `telemetry.raw.v1`, keyed by `robot_id`
  (6 partitions, 7-day retention). Producers use `acks=all` and idempotence, with
  a bounded buffer that blocks when full (backpressure).
* A separate projector process owns `fleet.robot_latest_state`. Per batch it
  commits state, quarantine rows **and its next offset per partition**
  (`fleet.consumer_offsets`) in one PostgreSQL transaction. It seeks to the
  ledger on assignment. Kafka offsets are committed afterwards and are advisory.
* The ledger row is seeded and then locked `FOR UPDATE` at batch start. This
  fences a zombie consumer, including on a partition's very first batch.

## Alternatives
| Option | Why not |
|---|---|
| Commit Kafka offsets after the DB commit, no ledger | Correct only because the upsert is idempotent; redelivery still re-runs work and re-audits. Fine as a fallback (tested), weaker as the design. |
| Kafka transactions / exactly-once semantics | Covers Kafka→Kafka. Our sink is PostgreSQL, outside the Kafka transaction. |
| Inbox table of every processed `event_id` | Unbounded growth (432 M rows/day at design scale) for a guarantee the ordering key already gives. |
| Keep HTTP, add batching in the API | No buffering during DB outages, no replay, one consumer. |

## Consequences
* Effectively-once state changes over at-least-once delivery. Verified for
  crashes after commit, crashes inside a batch, racing consumers and producer
  restarts.
* The ledger holds one row per (group, topic, partition): bounded, no retention
  job. Dropping a group's rows makes that group re-read from Kafka's offsets
  (still safe: idempotent).
* The registry lookup by schema id is on the decode path. A registry outage must
  be, and is, classified as recoverable.
