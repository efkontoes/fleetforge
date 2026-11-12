# ADR 0003 — Transactional outbox; client idempotency keys for mission transitions

Status: accepted (Milestone 1)

## Context
A mission change must be both stored and announced. Writing to PostgreSQL and
then publishing to a broker is a dual write. A crash or outage between the two
leaves the database and the event stream inconsistent.

## Decision
* Every mission change commits three rows in one transaction:
  1. the version-guarded `UPDATE` of `missions`;
  2. an audit row in `mission_events`;
  3. an `outbox_events` row.
  In M2, Debezium publishes outbox rows from the WAL.
* The client supplies a `transition_id`. It is the primary key of the audit row
  *and* the outbox row. A retry finds the committed row and returns the original
  result (`replayed: true`) instead of applying the change again.
* `expected_version` provides optimistic concurrency: a stale view gets 409
  `VERSION_CONFLICT`.
* If a version-guarded `UPDATE` loses a race, the service checks the idempotency
  key once more. A concurrent retry of the same request has committed by then,
  because the `UPDATE` waited on its row lock.
* Outbox column meanings match Debezium's Outbox Event Router
  (`id / aggregatetype / aggregateid / type / payload`; the router's field names
  are configurable). The payload carries the mission `version` so consumers can
  drop stale or duplicate deliveries.

## Alternatives
| Option | Why rejected |
|---|---|
| Publish to Kafka after commit | The dual-write problem. |
| Two-phase commit / XA | Operationally heavy, poorly supported by Kafka. |
| CDC on `missions` directly (no outbox) | Leaks the table schema as the public event contract, and loses the intent ("why did it change?"). |
| Server-generated event IDs only | A retry after a lost response cannot be recognised as the same request. |

## Consequences
* Proven by `test_failure_midway_through_assignment_commits_nothing`. A failure
  injected after the `UPDATE` (verified visible inside the transaction) leaves no
  trace, and the retry then commits exactly once.
* Delivery from the outbox to Kafka will be at-least-once, so consumers must
  deduplicate on `event_id` or `version`.
* Outbox rows accumulate until M2 adds retention. Debezium reads the WAL, not
  the table, so rows can be deleted soon after commit.
