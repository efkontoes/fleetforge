# Milestone 2 — the engineering behind it

Fifteen concepts, each in the same shape: the problem, what breaks without the
mechanism, what we built, the alternatives, the trade-off, and a command you can
run to watch it happen. Numbers come from `benchmarks/milestone-2-report.md`.

Start the stack first: `make env && make up-full`.

---

## 1. Topics, partitions, offsets, consumer groups

**Problem.** 500 robots × 10 Hz must be consumed by several processes in parallel,
without any robot's events being processed out of order.

**Mechanism.** A *topic* is a set of append-only logs (*partitions*). Every record
gets a per-partition sequence number: its *offset*. A *consumer group* divides a
topic's partitions among its members; each partition has exactly one owner in a
group at a time. Order is guaranteed **within** a partition only.

**Ours.** `telemetry.raw.v1` has 6 partitions and key `robot_id`, so a robot's
events always land in one partition, in production order. Up to 6 projector
processes share the work (`PROJECTOR_REPLICAS`). The mission consumer is a
*different* group on a different topic: groups are independent readers of the
same data.

**Without it.** Random partitioning would spread one robot over partitions, and
two consumers could apply seq 102 before seq 101 for the same robot. The upsert
would still keep state correct (101 becomes STALE), but a valid observation would
be lost from the read model's point of view.

**Alternatives.** More partitions give more parallelism but more open files, longer
rebalances and smaller batches per partition. Changing the count later breaks
key→partition mapping for existing keys.

**Experiment.**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic telemetry.raw.v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group telemetry-projector
PROJECTOR_REPLICAS=3 docker compose --profile processing up -d   # watch partitions rebalance in the logs
```

## 2. At-least-once vs "exactly-once"

**Problem.** Every hop can fail after doing its work but before saying so: the
broker wrote the record but the ack was lost, or the projector committed to
PostgreSQL but crashed before committing its Kafka offset.

**Mechanism.** The only honest choice at each hop is to *retry*. That means
duplicates (at-least-once). Kafka's idempotent producer removes duplicates
created by *its own retries within one producer session*. Kafka transactions give
exactly-once *between Kafka topics*. Neither covers "Kafka → PostgreSQL", and
neither covers a producer process that restarts.

**Ours.** At-least-once everywhere, plus effectively-once *effects*:
* the offset ledger (a record already applied is `SKIPPED`);
* the ordering key (a redelivered event is `DUPLICATE` or `STALE`);
* version guards (mission events).

**Without it.** A crash in the window "DB committed, offset not committed" would
re-apply a batch: double `state_version` bumps and duplicate audit rows.

**Experiment.** `pytest tests/stack/test_projector_recovery.py -k crash_after_db_commit`
runs it twice:
* resuming from the ledger: 0 records re-read;
* trusting Kafka's offsets: the whole batch is redelivered and every record is `SKIPPED`.

`tests/stack/test_kafka_ingestion.py -k producer_restart` shows a restarted
producer re-sending already-acknowledged events, with the final state still
equal to the oracle.

## 3. Idempotency and transaction boundaries

**Problem.** "Apply this event" must be safe to repeat, and each consumer's
"I have done it" must be durable at the same instant as the effect.

**Ours.** The transaction boundary is the batch. State rows, quarantine rows and
the ledger row commit together or not at all. The idempotency key is the ordering
key `(boot_number, sequence_number)` plus `event_id`, not a list of seen ids.

**Without it.** Writing the ledger in a separate transaction recreates the
dual-write problem inside our own database.

**Alternatives.** An inbox table of processed `event_id`s gives exact deduplication
but grows without bound (432 M rows/day at design scale). It belongs in the
lakehouse (M3), not the operational store.

**Experiment.** `pytest tests/integration/test_projector_processor.py -k "redelivery or crash_during"`.

## 4. Batching and write amplification

**Problem.** Each durable commit waits for a WAL flush (~4.6 ms here). One commit
per event caps a single writer at ~200 events/s no matter how fast the code is.

**Mechanism.** Amortize: one flush per *batch*. Round trips matter too: 500
single-row statements in one transaction still cost 500 network round trips.

**Ours.** The `set` mode uses a constant number of statements per batch (ledger,
lock, `unnest()` upsert, audit insert, ledger), and only one state write per
robot per batch: the planner collapses superseded events. That also reduces
*write amplification*: fewer heap tuples, fewer index updates, fewer WAL records
per event (measured 6.13 → 3.33 → 1.06 from `per_event` to `loop` to `set`).

**Measure WAL carefully.** WAL *bytes* per event over a short window mislead.
After every checkpoint, the first change to each data page writes the whole
8 KB page (a *full-page image*, which protects against torn pages). Whether a
checkpoint falls inside a 15,000-event window can swing bytes per event by
several times. An earlier version of this milestone reported "653 → 80 B/event"
and the audit could not reproduce it. The benchmark now forces a `CHECKPOINT`
first and reports `pg_stat_wal` records and full-page images separately. The
operational number comes from long runs: ~335 B/event, 1.7 MB/s at 5,000/s
sustained. That is the figure that sizes replication-slot headroom.

**Trade-offs.**
* Bigger batches mean more latency per event and bigger transactions to redo on
  failure.
* Database constraints only see written rows. In `set` mode an invalid event
  superseded later in the same batch never meets the `CHECK`, so validation in
  the application is mandatory (`test_partial_batch_failure_isolates_the_poison_record`).

**Experiment.** `python benchmarks/m2_benchmark.py drain --modes per_event:1 loop:500 set:1 set:50 set:500`.

## 5. PostgreSQL WAL and durability

**Problem.** "Committed" must survive a power cut.

**Mechanism.** Before `COMMIT` returns, PostgreSQL writes the change to the
write-ahead log and `fsync`s it. Data pages are written later; after a crash the
WAL is replayed. `synchronous_commit=off` returns before the flush (a ~200 ms risk
window): we keep it **on**.

**Ours, measured.**
* `pgbench`: 4.6 ms per durable commit, 0.13 ms with `synchronous_commit=off` (M1).
* `dd oflag=dsync` on the WSL disk: ~2 ms per synced 8 KB write.
* `pool_pre_ping` adds one autocommit `SELECT 1` per pool checkout: one more round
  trip per HTTP event, and it shows up in `xact_commit` (2.0 commits per HTTP event;
  measured by disposing the engine so the backend flushes its statistics).

Also learned: `pg_stat_database` counters are flushed per backend with up to
~10 s delay (PostgreSQL 15+), so the benchmark waits before reading them.

**Experiment.**
```bash
docker compose exec postgres pgbench -U fleetforge -n -c1 -T5 -f <(echo "select 1") fleetforge
```

## 6. Change Data Capture

**Problem.** Other systems need to know what changed in PostgreSQL, in commit
order, without the API calling them.

**Mechanism.** Logical decoding turns the WAL into row changes. A *publication*
selects tables (and operations). A *replication slot* remembers how far a
consumer has confirmed, and **pins WAL** until then. Debezium reads the slot and
writes Kafka records.

**Ours.**
* Two publications with two slots. The outbox is insert-only; firmware contracts
  get full insert/update/delete with before and after images (REPLICA IDENTITY FULL).
* Debezium's position lives in Kafka (`_connect_offsets`) and the slot's
  `confirmed_flush_lsn`.
* `max_slot_wal_keep_size=2GB`, plus an alert at 512 MiB of retained WAL.

**Without the slot cap.** A stopped connector makes PostgreSQL keep WAL forever,
and the disk fills. With the cap, the *slot* breaks instead, and CDC needs a
re-snapshot: a recoverable incident instead of a database outage.

**The subtler trap we measured: quiet tables pin busy WAL.** After the benchmarks
(telemetry only, Debezium stopped), both slots retained **531 MB**, and the
reference slot did not advance at all after Debezium restarted. A slot moves only
when its consumer confirms an LSN, and Debezium confirms only after receiving a
change from its publication. The outbox and firmware tables were quiet, so all
the telemetry WAL around them stayed pinned. Fix: a heartbeat table in both
publications and `heartbeat.action.query` inserting into it every 10 s
(migration 0007). Result: 531 MB → a few kB once the backlog was decoded.
`test_quiet_captured_tables_do_not_pin_wal_under_telemetry_load` holds it there
under load. The outbox retention job also refuses to delete while the slot is
inactive or lagging: it refused, correctly, while the 531 MB were pinned.

**Experiment.**
```bash
docker compose stop connect
# create missions via the API, then:
docker compose exec postgres psql -U fleetforge -d fleetforge -c \
  "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) from pg_replication_slots"
docker compose start connect   # events arrive, retained WAL drops
```
`tests/stack/test_cdc.py -k debezium_restart` automates this. A clean Connect
restart measured 43 s, almost all of it JVM startup and plugin scanning (the
image bundles 13 connectors). Restricting `plugin.path` to the Postgres connector
broke class loading in this image, so it was reverted.

## 7. Transactional outbox

**Problem.** "Update the mission and tell the world" is two writes to two systems.

**Ours.** The mission row, the audit row and the outbox row commit in one
transaction (M1). Debezium turns committed outbox rows into Kafka events (M2).
A rolled-back change has no outbox row, so no event
(`test_rolled_back_change_never_reaches_kafka`).

**Outbox ≠ table CDC.** The outbox carries *business events* the API chose to
publish ("MissionStatusChanged v2→v3"). Table CDC carries *row changes* ("column
X went from A to B"). The outbox is append-only, so retention deletes must be
invisible: `publish = 'insert'` (tested by `test_outbox_cleanup_is_invisible_to_cdc`).

## 8. Backpressure and consumer lag

**Problem.** The database is down for 7 seconds, and telemetry keeps coming.

**Ours.** Kafka absorbs the burst (that is its job). The projector classifies the
error as recoverable, pauses its partitions, backs off exponentially, keeps its
group membership by polling, and does **not** advance offsets or quarantine
anything. Lag grows, which is the visible backpressure signal. On the producer
side, a full local buffer makes `produce()` block instead of growing memory.

**Measured.** With a 7 s outage, 1,592 records accumulated; they were drained
3.2 s after PostgreSQL returned, with the final state matching the oracle
(`test_database_restart_while_kafka_buffers`).

**Without it.** Treating "DB down" as bad data would quarantine valid events, and
crashing would turn into a restart loop. We nearly shipped a subtler version:
registry errors were initially classified as malformed payloads.

## 9. Schema evolution

**Problem.** Producers and consumers deploy at different times.

**Ours.**
* Avro with Apicurio's Confluent-compatible API, `BACKWARD` compatibility on
  `telemetry.raw.v1-value`.
* Producers never auto-register; `fleet-schemas register` is the deployment
  gate, and `fleet-schemas check` is the dry run.
* Consumers decode with the *writer's* schema (looked up by the id in each
  message), then apply their own contract checks (`observation_from_record`).
* Business `schema_version` (contract major version) is distinct from the
  registry's schema id (transport).

**Demonstrated (`test_schema_evolution_on_the_real_registry`).**
* v2 adds an optional `heading_rad` and is accepted.
* v3 turns `battery_pct` into a string and is rejected with HTTP 409.
* v2 events are processed by the v1-era projector.
* Malformed bytes are quarantined while the partition keeps moving.

**Failure we hit.** Schema ids live inside every message, so the registry must be
at least as durable and available as the data. It now stores schemas in Kafka
(KafkaSQL), not in PostgreSQL.

## 10. Quarantine vs dead-letter queue

**Problem.** One undecodable record must not stop a partition, and must not be lost.

**Ours.** A quarantine *table*, written in the same transaction as the ledger.
Each row carries:
* source topic, partition and offset;
* the raw bytes and the decoded payload;
* category, reason, retry eligibility, status and consumer version.

A DLQ *topic* would be a second, non-atomic write and harder to query. The
inspection is plain SQL:
```sql
select reason_code, status, count(*) from fleet.telemetry_quarantine group by 1, 2;
```
A DLQ is not a universal answer. Infrastructure errors must never go there;
they are retried in place.

**The hard part is the classifier, and it must fail safe.** The audit found
valid events quarantined as "malformed" while the schema registry restarted.
The classifier recognised infrastructure failures by a list of exception
*names*, and `httpx.ReadError` ("connection reset by peer") was not on it.
Anything it did not recognise defaulted to "the record is bad", which is the
expensive mistake. A permanent verdict needs positive evidence about the record.
Now every transport error class counts as infrastructure, and even a 404
"unknown schema id" is permanent only while the registry demonstrably holds the
topic's subject. Getting it wrong in the safe direction costs a stall that pages
someone. Getting it wrong the other way costs silent, permanent loss that a
replay must later undo. A second lesson: my first fix targeted a plausible
cause (404s while the registry loads) without checking the quarantined rows'
own error messages. The tests passed three times, then failed again. Read the
evidence before the hypothesis.

## 11. Recovery, replay, reconciliation

**Ours.** `fleet-replay` reruns events through the live processor:
* `event` — a single event;
* `offsets` — an offset range;
* `quarantine` — every retry-eligible quarantined event;
* `correct` — a corrected event.

Every mode supports `--dry-run`, which executes and then rolls back. Status
changes commit with the state change, and every run is audited in
`fleet.replay_runs`. Replays cannot overwrite newer state: the conditional upsert
makes an old event `STALE`.

**Experiment.**
```bash
fleet-replay quarantine --reason UNKNOWN_ROBOT --dry-run
fleet-replay correct --event-id <id> --set battery_pct=55 --dry-run
```
Reconciliation in M2 is "replay from Kafka within retention". Full rebuilds from
history arrive with the bronze layer (M3).

## 12. Operational vs analytical consistency

The read model answers "what is robot R doing **now**?" It is current within the
event→state latency (measured p50/p95/p99 in the report), keeps one row per robot,
and deliberately forgets history. Analytical questions ("battery curve over a
mission", "how often are events late?") need **every** observation, including the
stale and duplicate ones. Those live in Kafka for 7 days and will be preserved in
bronze/silver (M3). The two views are not supposed to agree row for row: they
answer different questions with different consistency contracts.

---

The last three sections come from the final audit. Each one started as a bug or
a wrong explanation in an earlier version of this milestone.

## 13. An offset only means something in one incarnation of a log

**Problem.** "Offset 50,000 of `telemetry.raw.v1`" names a position in one
specific log. Delete and recreate the topic and the name survives, but the
offsets restart at 0. Retention can also delete the head of a log before a slow
consumer reads it.

**What breaks without a check.** The projector seeks to its ledger offset. If that
offset is past the end of the new log, librdkafka quietly resets to the earliest
offset, and every record below 50,000 is "already applied" and skipped. If the
new log is longer than 50,000, the seek succeeds in the middle of a log the
projector has never read. Neither case raises an error. The audit reproduced
both against a real broker.

**What we built** (ADR 0011). The ledger row stores Kafka's **topic UUID**, which
changes on recreation. On assignment the consumer requires `low ≤ ledger ≤ high`.
Inside the batch transaction it checks that the batch starts exactly where the
ledger says, because offsets are contiguous on a non-transactional topic.
Anything else stops the consumer with `OffsetLedgerError`. An operator records
the decision with `fleet-replay ledger-reset`, which is audited.

**Trade-off.** We chose to stop rather than guess. A stopped consumer pages
someone; a silently skipped million events are found weeks later, if ever.

**Experiment.**
```bash
FLEETFORGE_STACK=1 pytest tests/stack/test_ledger_recovery.py -s
```

## 14. Finding a latency tail: one clock, every hop, and the other process's logs

**Problem.** One run in four at 5,000 events/s had a p99 of 2.1 s. The first
explanation ("a producer→broker stall") described the symptom, not the cause.

**Method.**
1. **One clock.** Each hop is timestamped with `time.monotonic_ns()`.
   CLOCK_MONOTONIC is system-wide on Linux, so producer, projector and harness
   processes on one host share a timeline. Kafka's millisecond wall-clock
   CreateTime is not mixed in. (Across hosts you would need synchronized clocks,
   or you measure only round trips.)
2. **Measure from the schedule, not from the send.** A paced producer that falls
   behind sends late. If the clock starts at `produce()`, the delay disappears:
   this is *coordinated omission*. The harness keeps the scheduled send time of
   every event.
3. **Split the path:** scheduled → enqueued → acked → received → batch →
   tx_begin → pre_commit → committed. The segment that grows is where to look.
4. **Read the logs of the process that froze.** Here every hop that touched the
   broker grew at the same moment. The broker's GC log showed **Full GC pauses
   of 2,104 ms and 11,773 ms**. The first coincides with the old 2,104.56 ms p99
   to within a millisecond. The JVM had picked the single-threaded **Serial**
   collector, because a 768 MiB container looks like a small machine to it.
5. **`Real` vs `User+Sys` in the GC log.** The 11.8 s pause used 2.9 s of CPU.
   The rest was waiting: the heap had been swapped out, and swap was full. A GC
   pause longer than its CPU time points at memory pressure or CPU starvation,
   not at the collector being slow.

**Fix** (smallest correction): Kafka's documented G1 settings
(`-XX:+UseG1GC -XX:MaxGCPauseMillis=20`) and no swap for the broker container.
The before/after numbers are in the benchmark report.

**Lesson.** A dependency's pause shows up in *your* latency. Read its logs before
tuning your own code.

## 15. Committed is not delivered: measure CDC completeness end to end

**Problem.** The outbox guarantees that a committed mission change has an
outbox row (one transaction). It does **not** guarantee that a consumer ever
sees it: that depends on Debezium, the replication slot and Kafka.

**What breaks.** Connector state can say RUNNING while nothing moves. A latency
histogram of *delivered* events goes silent during a total stall, so an alert
on its p95 never fires exactly when it matters.

**What we built.** The ops exporter compares the source of truth
(`fleet.missions.version`) with the downstream read model
(`downstream.mission_status.version`) and exports how many missions are behind
and for how long. `CdcDeliveryStalled` alerts on that. Slot health is exported
too (`safe_wal_size`, `wal_status = lost`), because a lost slot means streaming
cannot resume and only a re-snapshot of rows still in the outbox can recover.

**Experiment.**
```bash
FLEETFORGE_STACK=1 pytest tests/stack/test_cdc.py -m disruptive -k killed -s
# kills Kafka Connect during mission traffic; prints detection and recovery times
```
