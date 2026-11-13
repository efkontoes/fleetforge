# ADR 0011 — The offset ledger is bound to one incarnation of the log

Status: accepted (Milestone 2 final audit). Amends ADR 0006.

## Context
ADR 0006 made `fleet.consumer_offsets` the source of truth for "what has been
applied". The audit found that the ledger was trusted without checking that it
still described the log it was read against. Three situations made the projector
lose data silently:

| Situation | What happened before |
|---|---|
| Topic deleted and recreated (same name), projector restarted before the new log passed the old ledger offset | Seek beyond the end, out of range. librdkafka reset to the earliest offset, and every new record below the old ledger offset was marked `SKIPPED` ("already applied"). |
| Same, but the new log had already passed the old offset | The seek landed in the middle of the new log. Everything before it was never read, and no error was raised. |
| Retention deleted records the projector had not applied yet (it fell more than 7 days behind) | librdkafka reset to the earliest available offset. The gap was never reported. |

Each of these can only be recovered by a human decision: replay from an archive,
accept the loss, or reprocess. Guessing a position is not correct.

## Decision
* **Topic identity.** Each ledger row stores Kafka's topic UUID
  (`kafka_topic_id`, migration 0008). The UUID changes when a topic is recreated,
  even though the name stays the same. The batch transaction compares it while
  holding the ledger row lock. A legacy row with no UUID adopts the current one
  on its next commit.
* **Watermark check on assignment** (`fleetforge.kafka.offsets`). Before seeking,
  the consumer requires `low ≤ next_offset ≤ high`. A committed row is honoured
  even when it is 0, because that value only comes from an operator's reset.
* **Gap check inside the transaction.** `telemetry.raw.v1` is written without
  Kafka transactions, so its offsets are contiguous. A batch that starts beyond
  the ledger means records were never delivered. This catches retention
  overtaking a consumer *while it runs*, which the assignment check cannot see.
* **All three raise `OffsetLedgerError`.** It is never retried and never
  quarantined: the consumer stops, and its alerts (lag, staleness, process down)
  fire.
* **Recovery is audited.** `fleet-replay ledger-reset --group --partition --to
  --reason` validates the target against the current log, rebinds the row to the
  current topic UUID, and writes a `LEDGER_RESET` row to `fleet.replay_runs` in
  the same transaction. `--dry-run` rolls back but keeps the audit row.
* The mission consumer gets the same checks. Its malformed events, which were
  previously only logged, now go to `downstream.mission_event_quarantine` with
  their Kafka coordinates and bytes, in the transaction that moves its ledger
  past them.

## Alternatives
| Option | Why not |
|---|---|
| `auto.offset.reset=none`/`error` only | Catches out-of-range seeks. It misses a recreated topic that has already grown past the old offset, and librdkafka reports the condition as an asynchronous consumer error, not at a point we can act on transactionally. |
| Auto-heal: reset to the earliest offset and log a warning | This is the silent loss we are removing. A warning is not a decision. |
| Name topics with an incarnation suffix (`telemetry.raw.v1.<n>`) | Helps with deliberate recreation, but not with accidental recreation or retention gaps. |

## Consequences
* Recreating a topic, or letting a consumer fall behind retention, now **stops
  that consumer** until an operator runs `ledger-reset`. This is deliberate:
  availability is traded for not losing data silently.
* Producers to `telemetry.raw.v1` must not use Kafka transactions. Control
  records would leave offset gaps and trip the gap check. This is a written
  contract (runbook); a transactional producer would need the gap check to
  consult the log's actual offsets.
* Tests: `tests/integration/test_offset_ledger.py` (identity, gaps in every mode,
  legacy rows, mission quarantine) and `tests/stack/test_ledger_recovery.py`
  (real topic recreation, a ledger ahead of the log, `delete_records` as
  retention, reset to 0, and the audited reset).
