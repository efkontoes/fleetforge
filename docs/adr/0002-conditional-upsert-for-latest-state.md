# ADR 0002 — Maintain latest state with a single conditional upsert

Status: accepted (Milestone 1)

## Context
Several writers (API threads now, Kafka consumers later) can apply events for
the same robot at the same moment. Whether to apply an event depends on the
current row, so "check, then write" must be atomic.

## Decision
One statement decides and writes:

```sql
INSERT ... ON CONFLICT (robot_id) DO UPDATE SET ...
WHERE (EXCLUDED.boot_number, EXCLUDED.sequence_number) > (s.boot_number, s.sequence_number)
RETURNING state_version
```

At READ COMMITTED, PostgreSQL locks the conflicting row and evaluates the
`WHERE` against its latest committed version. A concurrent insert of a missing
row waits on the speculative insertion and then takes the update path.

If nothing is returned, a follow-up read classifies the event as
DUPLICATE, STALE or CONFLICT, for the response and the audit trail only.

## Alternatives
| Option | Why rejected |
|---|---|
| `SELECT` then `UPDATE` in application code | Lost update. Demonstrated by `test_naive_read_then_write_loses_the_newer_update`. |
| `SELECT ... FOR UPDATE`, decide, then write | Correct, but two round trips while holding the lock, plus more code to get wrong. |
| SERIALIZABLE isolation | Correct, but the losers get serialization failures that need retry loops. |
| Append-only event table + "latest" view | Unbounded growth at 5,000 events/s. That is the lakehouse's job (M2). |

## Consequences
* Correctness is proven with forced interleavings (`test_conditional_upsert_under_forced_race`),
  not with timing-dependent thread races.
* The classification read happens after the decision. If a newer event commits
  in between, a true duplicate can be *labelled* STALE. The state is unaffected.
* Writes to one robot are serialized by its row lock, so one robot's throughput
  is bounded by commit latency. That is fine at 10 Hz per robot.
* Postgres does not remember every event ID. A duplicate of an *old* event is
  treated as STALE, which is correct for current state. Exact historical
  deduplication belongs to Silver (M2).
