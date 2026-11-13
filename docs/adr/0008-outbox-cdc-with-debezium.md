# ADR 0008 — Debezium on dedicated publications; registry on KafkaSQL

Status: accepted (Milestone 2)

## Decision
* Two Debezium PostgreSQL connectors (`pgoutput`), each with its own slot and a
  pre-created publication:
  * `fleetforge_outbox` — `fleet.outbox_events`, **publish = insert**, EventRouter
    to `fleet.<aggregate>-events.v1`, keyed by aggregate id, payload expanded,
    event type in a header.
  * `fleetforge_reference` — `fleet.firmware_versions`, all operations, REPLICA
    IDENTITY FULL, compacted topic with tombstones.
* `snapshot.mode=initial`, `slot.drop.on.stop=false`, heartbeats every 10 s.
  Credentials come from `${env:CDC_DB_PASSWORD}` (EnvVarConfigProvider), so no
  secret is stored in connector configs or the repository.
* `max_slot_wal_keep_size=2GB` plus alerting on retained WAL.
* Apicurio Registry stores schemas in Kafka (KafkaSQL), not in the operational
  PostgreSQL.

## Heartbeat action query (added after measurement)
After a benchmark wrote ~1 GB of telemetry WAL with Debezium stopped, both slots
pinned **531 MB**, and the reference slot did not advance at all after Debezium
came back. A slot is confirmed only when Debezium receives a change from its own
publication, and the outbox and firmware tables were quiet. Migration 0007 adds
`fleet.cdc_heartbeat`, which is in both publications; each connector INSERTs into
it every 10 s (`heartbeat.action.query`; INSERT because the outbox publication is
insert-only). Measured afterwards: 531 MB → a few kB once the backlog was decoded,
and `test_quiet_captured_tables_do_not_pin_wal_under_telemetry_load` holds the
slots under 10 % of the WAL produced by a 50,000-event load. Heartbeat rows are
trimmed by the retention job.

## Why the registry moved
The first version stored schemas in PostgreSQL. A database-restart test showed
that this put the registry in the database's failure domain: producers could not
serialize, and a cold consumer's decode error was being classified as a malformed
payload, i.e. **valid events quarantined because of an outage**. Fixed twice:
registry errors are now recoverable, and the registry lives next to the messages
whose schema ids it serves.

## Alternatives
* Polling the outbox table: simple, but adds latency and load, and ordering
  under concurrent commits is subtle. CDC reads commit order from the WAL.
* One connector capturing both tables: one slot, but the outbox would then
  publish deletes, and outbox and table CDC have different semantics.
* Avro for CDC events: deferred. JSON keeps the outbox payload as written by the
  API; the contract is the payload's `version`/`event_id`.
