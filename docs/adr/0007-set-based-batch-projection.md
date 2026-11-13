# ADR 0007 — Set-based batch projection with a sequential-equivalent planner

Status: accepted (Milestone 2). Numbers: benchmarks/milestone-2-report.md

## Context
One durable commit per event costs ~4.6 ms on this disk: the dominant cost in
Milestone 1. Batching amortizes it, but a batch can contain several events for
the same robot, and the per-event outcomes and `state_version` must stay
identical to one-at-a-time processing.

## Decision
* `domain.ingest.plan_batch` (pure) replays a batch in delivery order against the
  state locked at batch start: per-event decisions, one winner per robot, and the
  number of applied events.
* The database receives a constant number of statements per batch: ledger seed and
  lock, state lock, one multi-row conditional upsert (`unnest` arrays,
  `RETURNING`), one multi-row audit insert, one ledger upsert, one COMMIT.
* The conditional `WHERE (new) > (held)` remains in the upsert. A mismatch between
  `RETURNING` and the plan means a concurrent writer, so roll back and retry.
* Three modes are kept for measurement: `per_event`, `loop` (one transaction, one
  statement per event) and `set`, plus `set_copy` (COPY into a temp table).

## Alternatives
* **Loop of single-row statements inside one transaction:** 896–1,128 events/s
  vs 7,680–8,591 (set), with the same number of commits. Still one round trip
  per event. (The original measurement, 915 vs 5,224, predates the audit's
  projector CPU fix.)
* **COPY + INSERT…SELECT:** 8,106–8,891 events/s vs 7,680–8,591 with
  `unnest()`. Within run-to-run noise, it writes more WAL records per event
  (1.52 vs 1.06) and adds a temp-table insert. Not adopted at these batch sizes.
* **Write only the max per robot, without planning:** correct final row, wrong
  outcomes and versions.

## Consequences
* Database constraints only see written rows. An invalid event superseded later in
  the same batch never reaches a CHECK constraint, so application validation is
  mandatory (shown by `test_partial_batch_failure_isolates_the_poison_record`).
* A database-refused batch falls back to per-record processing to isolate poison.
