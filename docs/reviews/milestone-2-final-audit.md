# Milestone 2 — final audit

Date: 2026-11-12/13. Scope: branch `milestone-2-distributed-ingestion`, PR #1.
This is an independent audit of the implementation against its documented
guarantees. It is not a code review. Every claim below points at a test, a
measurement or a raw result file. Where something could not be verified, this
document says so.

**Summary.** The audit found **eight defects** and fixed them, and hit **one incident whose root cause it could not determine** (#9; mitigated, and diagnosable next time):

| # | Defect | Severity | Fix | Evidence |
|---|---|---|---|---|
| 1 | Offset ledger trusted without checking it still describes the log: a recreated topic, a ledger ahead of the log, or records deleted by retention were **skipped silently** (data loss) | high | ledger bound to the Kafka topic UUID, watermark and gap checks, fail closed, audited `ledger-reset` (ADR 0011, migration 0008) | `tests/integration/test_offset_ledger.py`, `tests/stack/test_ledger_recovery.py` |
| 2 | Latency tail (2.1 s, and 11.8 s in this audit) caused by **Serial-GC Full collections in the Kafka broker**, stretched by swapping. The earlier report blamed a "producer→broker stall" | high (availability/latency) | G1 + no swap for the broker | `benchmarks/results/m2-kafka-broker-gc-serial-excerpt.txt`, traced runs §8 |
| 3 | Projector CPU ~130 µs/event in decode/validate (≈25 % spent on Confluent per-message rule/tag machinery we do not use) and a 60–160 ms full GC every ~2.5 s, which left no headroom at 5,000/s | medium | direct fastavro decode (proved byte-identical), lazy audit payload, `gc.freeze()` | `benchmarks/projector_gc.py`, `TestDecoderEquivalence` |
| 4 | CDC stall detection blind spot: the only delivery alert was computed from *delivered* events, so it was silent during a total stall; slot invalidation was not monitored | medium | end-to-end completeness metric (`missions` vs downstream versions) + slot `safe_wal_size`/`wal_status` + 3 alerts | `test_debezium_killed_during_mission_updates_is_detected_and_recovers` |
| 5 | Malformed mission events were only logged (no coordinates, no bytes) | low | `downstream.mission_event_quarantine`, same transaction as the ledger | `test_malformed_mission_event_is_quarantined_with_coordinates` |
| 6 | The projector and mission-consumer roles could rewrite **each other's** offset ledger; the projector had an unused grant | medium (least privilege) | row-level security by topic; grant revoked | `test_consumers_cannot_touch_each_others_offset_ledger[*]` |
| 7 | **A restarting schema registry turned valid events into permanent "malformed" data.** The registry classifier matched transport errors by a list of class *names* that lacked `httpx.ReadError`, so "Connection reset by peer" from a restarting registry quarantined valid events (reproduced: 4 events, 2 robots' `state_version` wrong). It predates the audit. My first fix in this audit targeted a hypothesised 404-while-loading cause and was wrong; the quarantine rows' own error messages showed the real one | high | classify by exception class (`httpx.TransportError`, `OSError`, timeouts); a 404 is permanent only while the registry holds the topic's subject (defensive, ADR 0009) | `test_registry_outage_pauses_projection_and_never_quarantines` (repeated runs), decoder unit tests |
| 8 | CI and test harness: ruff version mismatch, mutable action tags, unpinned service image, missing timeouts; stack tests flaky on GitHub runners (`_DESTROY_BROKER`, `_UNKNOWN_PARTITION` from fresh clients); a failed wait could hang a job for 30–40 min; the test-only idle stop counted group-join time | medium (delivery) | pins, timeouts, bounded retry of one-shot admin/metadata calls (also used by the library), daemon threads, pytest-timeout + faulthandler, idle = no records while owning partitions | CI runs §11 |
| 9 | **The Compose projector wedged after the disruptive suite:** alive, out of its consumer group for ~25 min, nothing logged after a session-timeout revoke, never restarted, while every projector-side alert stayed silent (its own metrics stop when it stops). Kernel evidence: main thread in a timed futex wait. **Root cause not determined**: two reproductions (Kafka down beyond the session timeout, with and without PostgreSQL down during the rejoin) recovered normally, and attaching a debugger to the container was not permitted | high (availability) | mitigations: the advisory Kafka commit can no longer raise out of the loop; bounded `close()` so a failing process always exits; exit 1 with a logged traceback on any error; `restart: unless-stopped` on long-running services (there was none); `ProjectorNotInGroup` / `MissionConsumerNotInGroup` alerts computed by kafka-exporter; `faulthandler` on SIGUSR1 (`docker compose kill -s USR1 projector` dumps all stacks, verified) | the stuck state was seen once, in local evidence run 3; the group was Stable after the final disruptive suite (§9) |

Three methodology errors in my own earlier benchmarks were also found:
measuring from `produce()` (coordinated omission), a warm-up trim that only
trimmed one consumer, and "kept up" judged by p99 instead of lag over time.
Section 6 explains them.

---

## 1. Verified architecture

```
robots ─Avro─▶ Kafka telemetry.raw.v1 (6 partitions, key = robot_id, 7 d)
                     │
                     ▼
        telemetry projector(s) ── ONE PostgreSQL transaction per batch ──▶ fleet.robot_latest_state
                                  (state + quarantine + offset ledger)      fleet.telemetry_rejections
                                                                            fleet.consumer_offsets
Fleet API ── mission change + outbox row, ONE transaction ──▶ PostgreSQL
                                                               │ WAL (pgoutput, slot fleetforge_outbox)
                                                               ▼
                                     Debezium 3.7 (Kafka Connect) ─▶ fleet.mission-events.v1 (key = mission)
                                                                         │
                                                                         ▼
                              mission consumer ── state + its ledger, ONE transaction ──▶ downstream.mission_status
```

What was verified against the running system, not the documents: topic layout
and keys (EventRouter keys by `aggregate_id`; the telemetry producer keys by
`robot_id`), connector configuration (`pgoutput`, insert-only publication,
`snapshot.mode=initial`, heartbeats), role privileges (queried from
`pg_roles`/`information_schema`, §10), and every transaction boundary below
(read from the code paths, then tested).

## 2. Transaction boundaries

| Unit of work | Contents (atomic) | Not in it |
|---|---|---|
| API mission transition | `missions` row (version + 1), `mission_events`, `outbox_events` | Kafka |
| API telemetry (legacy HTTP) | conditional upsert of latest state, or a rejection row | — |
| Projector batch | ledger lock (seed + `FOR UPDATE`, topic-UUID check, gap check) → registry lookups → `FOR UPDATE` of affected state rows → planned set-based upsert (`RETURNING` re-validated) → rejection rows → ledger advance (`GREATEST`) | Kafka offset commit (afterwards, advisory) |
| Poison isolation | the refused batch re-run one record per transaction; a record the DB still refuses is quarantined with its own ledger advance | — |
| Mission consumer batch | ledger lock → version-guarded upserts → quarantine of unparseable events → ledger advance | Kafka offset commit |
| Debezium | Kafka produce, then Connect offset flush; the slot's `confirmed_flush_lsn` only advances after that | anything in PostgreSQL |
| `ledger-reset` | ledger row (new offset + current topic UUID) and its `replay_runs` audit row | — |

## 3. Delivery guarantees

What each layer guarantees (no layer is "exactly once" end to end):

| Layer | Guarantee | Provided by |
|---|---|---|
| Producer → broker | at-least-once; no duplicates or reordering *within one producer session* (`acks=all`, `enable.idempotence=true`, max 5 in flight) | Kafka |
| Producer crash | **unacknowledged in-memory events are lost** (SIGKILL test); resending from the last contiguous acknowledged index turns that into duplicates. A real robot must persist that index | the producer, not Kafka |
| Broker outage longer than `message.timeout.ms` (300 s) | queued events become **reported** delivery failures, never silent loss | librdkafka |
| Broker durability | none beyond one disk (RF = 1) | local limitation |
| Broker → projector | at-least-once redelivery; per-partition order | Kafka |
| Projector → state | **effectively once** per event: the ledger skips redelivered offsets, and the ordering key plus conditional upsert make any replay idempotent | PostgreSQL (ledger, constraints, row locks) |
| Ledger ↔ log consistency | a disagreement stops the consumer; nothing is skipped silently | PostgreSQL + topic UUID / watermarks (ADR 0011) |
| Mission change → outbox | atomic: a committed change has its event, and a rolled-back one has none | PostgreSQL |
| Outbox → Kafka → consumer | **eventually**, at-least-once, ordered per mission while the slot is valid; consumers deduplicate by version | Debezium + slot + Kafka + version guard |

## 4. Offset recovery behaviour

Verified case by case (B2 of the brief):

| # | Requirement | Result | Test |
|---|---|---|---|
| 1 | A new partition's first batch cannot bypass the lock | seeded with `INSERT … ON CONFLICT DO NOTHING` before `FOR UPDATE`; a concurrent seeder blocks on the unique index | `test_two_consumers_racing_on_the_same_batch_apply_it_once` |
| 2 | Concurrent consumers cannot apply conflicting batches | the second blocks on the row lock, then skips | same; and the differential test redelivers tails to a second consumer |
| 3 | A failed DB transaction cannot advance the offset | the ledger advance is inside the same transaction | `test_crash_during_db_batch_leaves_no_trace`, `test_offset_gap_stops_the_consumer_without_committing[*]` |
| 4 | Crash after the DB commit, before the Kafka commit, is safe | resume from the ledger: 0 records re-read; trusting Kafka instead: all redelivered and `SKIPPED` | `test_crash_after_db_commit_before_offset_commit[*]` |
| 5 | Crash before the DB commit causes a retry | records are re-read from the ledger position | `test_consumer_terminated_mid_stream_and_restarted` |
| 6 | Rebalances cannot cause unsafe concurrent ownership | rebalance callbacks only run inside `consume()`/`poll()`, so a batch never straddles a revoke. A zombie that lost its partition is fenced by the ledger lock | code path + test 2 |
| 7 | A restarted consumer resumes from the durable position | seeks to the ledger on every assignment, after checking it against the log | tests 4 and 5 |
| 8 | A stale consumer cannot overwrite newer state | the ledger skips its offsets, and independently the conditional upsert rejects older ordering keys | the differential test; M1 ordering tests |
| 9 | Ledger and Kafka offsets stay consistent under replay | replays use a throwaway consumer group, never commit, and bypass the ledger (`consumer_group=None`) | `tests/integration/test_replay.py` |
| 10 | Offset retention cannot invalidate recovery | **was violated** (defect 1). Now: Kafka committed offsets are advisory and their retention is irrelevant; ledger rows are kept indefinitely; **log** retention overtaking the ledger stops the consumer | `test_records_deleted_before_being_applied_are_refused` |
| — | Topic recreation / truncation | **was violated** (defect 1); now refused until an audited reset | `test_recreated_topic_is_refused_until_an_audited_reset`, `test_legacy_ledger_ahead_of_the_log_is_refused` |

**Batch semantics (B3).** `test_random_batches_and_consumer_switches_equal_sequential_application`
generates hostile streams (10 % duplicates, 15 % reorders, invalid values,
missing fields, reboots, and injected same-sequence events with a different
identity, often in the same batch as their twin). It applies them once
sequentially (per-event mode, the M1 rule) and once in set mode, with random
batch boundaries (1–250), random switches between two consumer processes with
cold caches, and redelivered tails. Final state including `state_version`,
the rejection audit, and the outcome of every source record are identical for
three seeds. A planted bug (`state_version += 1` per batch instead of
`+= applied`) makes it fail.

## 5. CDC recovery behaviour

| Scenario | Result | Test |
|---|---|---|
| Rolled-back mission change | no outbox row, nothing in Kafka | `test_rolled_back_change_never_reaches_kafka` |
| Duplicate delivery | absorbed by the version guard | `test_repeated_delivery_is_absorbed_by_version_guard` |
| Connect stopped, changes committed meanwhile | slot retains the WAL; all events delivered in order after restart | `test_debezium_restart_resumes_from_committed_offset` |
| **Connect SIGKILLed during traffic** | the stall became visible end to end **17–25 s** after the kill (oldest undelivered change older than 15 s; 15 s scrape interval); all missions at their final version downstream **27–46 s** after restart (four kill runs; final evidence: 163/163). Detection range is from the two runs using the age criterion (mostly Connect's JVM start). An earlier version of this test used "missions behind > 0", which is also non-zero during normal traffic, and was replaced | `test_debezium_killed_during_mission_updates_is_detected_and_recovers` |
| PostgreSQL restart | connector recovers by itself | `test_postgres_restart_cdc_recovers` |
| Outbox cleanup | deletes are not published (`publish = 'insert'`) | `test_outbox_cleanup_is_invisible_to_cdc` |
| Quiet captured tables under telemetry load | heartbeats keep slots advancing (was 531 MB pinned) | `test_quiet_captured_tables_do_not_pin_wal_under_telemetry_load` |
| Slot invalidated (`max_slot_wal_keep_size`) | **not tested destructively.** Detected (`ReplicationSlotLost`), and recovery is a re-snapshot of rows still in the outbox (runbook). Retention refuses to delete while the slot is unhealthy | — |

"A committed outbox row exists" (atomic, §3) and "a consumer eventually
processes it" (asynchronous, depends on the slot) are separate guarantees. The
second is now *measured continuously*. After all audit test runs, every one of
the **696 missions** ever committed on the audit stack was at its source version
in `downstream.mission_status` (0 behind; checked directly in PostgreSQL).

## 6. Benchmark methodology

Inspected: `benchmarks/m2_benchmark.py` (http, drain, sustained) and the
earlier report. Problems found in my own earlier method, all corrected before
interpreting results:

| Problem | Effect | Correction |
|---|---|---|
| Latency measured from Kafka CreateTime (`produce()`) | a producer behind schedule hid its own delay (coordinated omission) | measured from the **scheduled** send time; lateness reported separately |
| Warm-up trim applied to concatenated per-consumer lists | only consumer 1's first 10 % was trimmed | per-second time series; lag judged after a warm-up window |
| "Kept up" = p99 < 2 s | not a stability criterion | **durable lag** (high watermark − PostgreSQL ledger) every second; mean in first vs second half; drain after stop |
| Workload pre-generated in memory | runs capped at ~30 s on 3.7 GiB | lazy generation per producer; 5-minute runs; streaming oracle |
| WAL bytes/event over 15 k-event windows | dominated by full-page images (checkpoint position) | `CHECKPOINT` before each window; `pg_stat_wal` records and FPIs reported |
| One number per run | cannot locate a tail | per-hop timestamps on one clock (`time.monotonic_ns()`, system-wide on Linux) joined by (partition, offset) |

Throughput is always **durable processing** (ledger advanced in PostgreSQL),
never Kafka acceptance. Duplicates and invalid events are included in the counts
(they are real load). The oracle check runs after the backlog has drained and
compares final state, per-robot `state_version`, and the exact quarantine rows.
Full details: `benchmarks/milestone-2-report.md`.

## 7. Sustained-throughput results

Core profile, 3 projector processes, `set:500`. Raw files:
`benchmarks/results/m2-*-traced-sustained-core-*.json`.

| Offered events/s | Duration | Event→state p99 | Lag mean, 1st → 2nd half | Verdict |
|---:|---:|---:|---:|---|
| 1,000 | 61 s | 190 ms | 57 → 56 | stable |
| 2,500 | 61 s | 127 ms | 136 → 147 | stable |
| **5,000** | **303 s** | **350 ms** (max 686) | **394 → 373** | **stable**; 1,515,049 events, all accounted for |
| 7,500 | 61 s | 262 ms | 602 → 862 | stable (drained in < 0.1 s) |
| **10,000** | 61 s | 470 ms | 1,431 → 1,386 | **stable** |
| 12,500 | 61 s | 1,650 ms | 1,715 → 11,984 | **not sustainable** (lag grows; host load 6.3) |
| 15,000 | 68 s | 23.6 s | 35,771 → 144,529 | overloaded (the producers themselves fall 8 s behind) |

* **Sustained:** 10,000 events/s is the highest tested offered rate with bounded
  lag (60 s), and 5,000 events/s held for 5 minutes. The 5,000/s design target
  is met with about 2× headroom on this laptop.
* **Peak:** 13,201 events/s (best 1-second window); 11,771 events/s backlog
  drain with 3 consumers.
* **Resources at 5,000/s:**
  * PostgreSQL: 72 commits/s, WAL 335 B/event (1.7 MB/s).
  * CPU: projector 0.27 cores per process; Kafka 0.43 cores; PostgreSQL 0.30 cores.
  * Memory: host `MemAvailable` ≥ 1.17 GiB.

**A–D, reproduced** on the original workload (15,000 events, 1 consumer):

| Arch | Earlier | Now | Speed-up vs A (now) |
|---|---:|---:|---:|
| A HTTP, 1 tx/event | 293 | 330 | 1× |
| B Kafka, 1 tx/event | 50 | 51–55 | **0.16×** |
| C Kafka, 1 tx/batch, per-event SQL | 915 | 896–1,128 | 2.7–3.4× |
| D Kafka, set-based batch 500 | 5,224 | **7,680–8,591** | **23–26×** |

Kafka alone (B) is slower than HTTP. HTTP had 32 concurrent writers sharing
group commit, while one consumer committing per event is a single serial writer
paying a ~5 ms WAL flush per event. Batching the transaction (C) removes the
per-event flush. Set-based SQL (D) removes per-event round trips and row
versions: WAL records per event drop 6.13 → 3.33 → 1.06. D is then bounded by
projector CPU, which the audit's decoder fix moved by +47–64 %. The earlier
"WAL 653 → 80 B/event" did not reproduce (full-page images dominate short
windows) and is marked as superseded.

## 8. Latency investigation

**Starting point.** Earlier report: one of four 5,000/s runs had a p99 of
2.1 s, attributed to "a producer→broker stall". That claim was re-examined
rather than accepted.

**C1 instrumentation.** `benchmarks/m2_traced.py` timestamps eight points per
sampled event on CLOCK_MONOTONIC: scheduled, enqueued (`produce()`), acked,
received (`consume()` returned), batch start, `tx_begin`, `pre_commit`,
committed. Commit returned is treated as visible: with
`synchronous_commit=on`, PostgreSQL makes the commit visible to new snapshots
before acknowledging it. All processes share one host kernel, so no
cross-host clock synchronization is involved, and resolution is
sub-microsecond. Also recorded: projector GC pauses (`gc.callbacks`),
rebalance callbacks, `consume()` gaps, per-process CPU from `/proc`, container
stats every 5 s, host `MemAvailable`, and durable lag every second.

**C2/C3: what the data showed** (18 controlled runs at 5,000/s, 60 s each,
a 5-minute run, and a 2,000/s smoke run; 3–5 runs per configuration):

| Candidate cause | Evidence | Verdict |
|---|---|---|
| Kafka broker GC | Serial collector in use ("Using Serial"). Full GCs of 2.10–11.77 s. **2,104.13 ms at 18:53:23 vs the old run's p99 of 2,104.56 ms.** In the smoke run, producer acks waited ~12 s, nothing committed for 11 s, and the monitor's metadata request timed out, all coinciding with an 11,772.8 ms Full GC (User 2.08 s + Sys 0.79 s vs Real 11.78 s: paging) | **root cause of the original tail and the largest freezes** |
| WSL memory pressure / swap | swap at 1,021–1,024 of 1,024 MB in the full profile. GC Real ≫ User+Sys. One episode had projector CPU at 0.02, commit 1.3 s and broker ack 0.73 s at the same moment: a host-wide stall | **amplifier** of 1; environmental |
| Projector CPU saturation + host contention | full profile: projector 0.85–0.95 cores per process, load 6–6.7 on 4 cores; `produce→receipt` (waiting in Kafka) dominated every remaining episode; durable lag up to 14,207 | **cause of the remaining 1–3 s tails** under pressure |
| Python GC in the projector | gen-2 every ~2.5 s, 60–160 ms (590 ms once) per process | contributor |
| Producer batching / linger / queue saturation | `linger.ms=20`; producer lateness p99 ≤ 14 ms in stable runs; 0 BufferError waits; ack p99 23 ms | not a cause below ~10,000/s (it is at 15,000/s) |
| Consumer poll interval / flush timer | `consume()` gap p50 ≈ 50 ms (= flush interval); receipt→batch p99 < 11 ms | baseline latency, not tails |
| PostgreSQL contention | SQL in transaction p99 65 ms, COMMIT p99 90 ms at 5,000/s; 54–86 commits/s | not a cause (spikes only during host stalls) |
| Rebalances | consumers join before production starts; no rebalance callback fell inside any outlier episode | not a cause |
| Load-generator overload | producers ≤ 0.6 cores each up to 7,500/s | not a cause below 12,500/s |
| Cold start | first 2–4 s of a fresh projector: one 1.9 s episode in 5 core runs | minor; once per restart |

**C4: fixes.** Broker on G1 plus no swap; projector decode 132 → 36 µs/event
and lazy audit payloads; `gc.freeze()`. Nothing unbounded was added and
nothing is dropped.

**Before → after** (5,000/s, 60 s each):

| | Before (Serial broker, old projector) | After (all fixes) |
|---|---|---|
| Full profile (memory pressure) | 5 runs: p99 557–3,120 ms, max 0.80–3.45 s, **4 runs with a > 1 s episode**, lag up to 14,207 | 5 runs: p99 200–621 ms, max 0.34–0.94 s, **0 episodes**, lag ≤ 1,835 |
| Core profile | 5 runs (G1 broker, old projector ⁵): p99 334–1,617 ms, 1 episode (cold start) | 3 runs: p99 206–482 ms, 0 episodes |
| 5-minute run | — | p99 350 ms, max 686 ms, 0 episodes, lag mean 394 → 373 |

⁵ Labelled "serialgc" when run; the broker had already been recreated with G1
(container start 23:34:12Z). Relabelled with a note in the files.

Not done, and why: a Serial vs G1 A/B with only the broker changed. Serial Full
GCs came every 20–60 minutes of load, so a 15-minute A/B would most likely show
nothing either way. The broker attribution rests on exact GC-log timestamps and
the paging signature. **The tail is reduced, not proven gone.** It was not seen
in 8 post-fix 60 s runs plus one 5-minute run at 5,000/s, but memory pressure
(full profile on 3.7 GiB) still produces sub-second spikes (max 0.94 s).

## 9. Failure-test evidence

All destructive tests ran against the local Compose stack only. Machine-readable
results (JUnit XML, with measured values recorded as test properties):
`docs/reviews/evidence/junit-unit-integration-failure.xml`,
`junit-stack-nondisruptive.xml`, `junit-stack-disruptive.xml`. Every scenario
ends with an oracle comparison of final state, per-robot `state_version`, and
(where relevant) quarantine rows, mission versions and durable offsets.

| Brief | Scenario | Measured | Verified |
|---|---|---|---|
| E1 | Projector crash after the DB commit, before the Kafka commit | resume from ledger: 0 records re-read; trusting Kafka's offsets instead: the batch is redelivered and every record is `SKIPPED` | state = oracle, no regression, no duplicate effects, ledger exact |
| E1 | Crash inside the DB transaction | nothing committed; the batch is re-read | state, audit and ledger unchanged |
| E2 | PostgreSQL stopped 6.6 s while Kafka kept accepting | backlog 1,588 events; 7 retries with backoff; partitions paused (no fetching, bounded memory: test process 120 MiB RSS); 0 records quarantined; **drained 1.9 s after restart** | state = oracle; no infrastructure error recorded as bad data |
| E3 | Kafka stopped 8 s during publishing | the producer buffers; no acknowledgement during the outage; all acknowledged once afterwards | state = oracle |
| E3 | Kafka stopped 10 s with `message.timeout.ms` = 3 s | **973 delivery failures reported** (not silent); resent from the checkpoint | state = oracle |
| E3 | Producer **SIGKILLed** (2 s linger) | 260 acknowledged, 780 in Kafka at kill time; the rest of the queue lost; resend from the last contiguous acknowledged index (3,205 events, duplicates absorbed) | state = oracle. Durable from broker ack; best-effort before it |
| E4 | Kafka Connect **SIGKILLed** during mission traffic (48 s down) | stall visible end to end 25 s after the kill; **163/163 missions** at their final version 42 s after restart | every mission's downstream version = source version |
| E4 | Connect stopped while changes commit; PostgreSQL restart under CDC | in-order delivery after restart; connector recovers by itself | event versions 0–3 in order |
| E5 | Malformed bytes, missing fields, incompatible schema (v3 rejected by the registry), unknown schema id, invalid values, DB-refused records | quarantined with topic/partition/offset, reason, category, bytes; the partition keeps moving; poison isolated per record | quarantine rows = oracle; `fleet-replay` dry run/replay/correction tested |
| E5 | **Schema registry stopped 11 s** with a cold projector cache | 9 retries, nothing committed, **0 quarantined**; projected 27 s after the registry returned | state and quarantine rows = oracle. This test found defect 7 |
| E6 | Reconciliation | every benchmark run and every scenario above compares against the oracle; offsets: ledger = high watermark at the end of every sustained run; CDC: 696/696 missions current | see each row |

The unit and integration suites add the ledger-identity, gap, RLS,
mission-quarantine and differential batch-equivalence tests (§4).

## 10. Security findings

**F1 — repository and GitHub.**
* `.env` is ignored and was never committed (0 commits touch it).
* None of the 7 generated secret values in `.env` appears anywhere in the full
  Git history or the working tree (checked by value, not printed).
* gitleaks over the full history passes in every CI run (`security` job), and
  pip-audit passes on `requirements.lock`.
* The GitHub token is stored in Windows Credential Manager via Git Credential
  Manager (`gh` reads it through a wrapper; no plain-text `hosts.yml`). The
  `workflow` scope was added through a device-code login whose temporary
  plain-text copy was deleted after the move.
* CI uses no repository secrets: `make-env.sh` generates throwaway values per
  run. The workflow runs with `permissions: contents: read`; actions are pinned
  by commit SHA and images by digest.

**F2 — database privileges** (re-tested: `tests/integration/test_roles.py`, 28 cases, all passing):

| Role | Can | Cannot (tested) |
|---|---|---|
| `fleetforge_app` | registry, missions, outbox, legacy telemetry path | touch ledger, delete outbox, update robot models |
| `fleetforge_projector` | latest state, rejections, **its own** ledger rows (`telemetry.*`), read registry | update missions, write outbox, delete state, read missions (revoked), touch mission-consumer ledger rows (RLS) |
| `fleetforge_mission_consumer` | `downstream.*`, **its own** ledger rows (`fleet.*-events.v*`), its quarantine | read telemetry state, touch projector ledger rows (RLS) |
| `fleetforge_cdc` | `REPLICATION`, read outbox/firmware, write heartbeats | read robots, write outbox |
| `fleetforge_readonly` | read everything, `pg_monitor` | any write |

Every role has a connection limit, and none is a superuser. **The database owner
`fleetforge` is still a superuser** (also `CREATEROLE`, `REPLICATION`,
`BYPASSRLS`). It is used only by migrations, provisioning, operator tools and
tests. Local implication: anything holding `.env` owns the cluster. Production
should use a non-superuser owner, and `rds_replication`-style membership for CDC.

**F3 — resources.** Container memory peaks (docker stats) and minimum host
headroom:

| Profile | Containers (peak) | Sum of peaks | Sum of limits | Observed |
|---|---|---:|---:|---|
| core | Kafka 495, PostgreSQL 416, registry 223, API 149 MiB | **~1.3 GiB** | 2.4 GiB | + benchmark (3 projectors ~113 MiB RSS each, 2 producers): `MemAvailable` ≥ 1.17 GiB, no swap pressure |
| processing | core + projector container 63 MiB | ~1.4 GiB | 2.7 GiB | — |
| cdc | core + Connect 621 MiB + mission consumer 47 MiB | ~2.0 GiB | 3.4 GiB | — |
| full | + Prometheus 48, Grafana 124, exporters 69 MiB | **~2.1 GiB** | 4.3 GiB | with the benchmark: swap 100 % used, `MemAvailable` down to ~280 MiB |

Recommended minimums on this host: **core/processing for benchmarks**
(`make up-bench`); the full profile for functional and stack tests, without
concurrent load generation. WSL memory was not raised (Windows had ~0.8 GiB
free). After testing, the stack is stopped with `make down`; volumes and
results are kept.

## 11. CI status

Workflow `.github/workflows/ci.yml`, triggered on every push to the PR:

| Job | What it checks | Notes |
|---|---|---|
| `lint` | ruff check + format (pinned 0.17.4); Compose config for every profile; promtool on Prometheus rules; Grafana dashboard regenerated from source | |
| `unit` | `tests/unit` | |
| `migrations` | `alembic upgrade head → downgrade base → upgrade head` on PostgreSQL 17 | every migration reversible |
| `integration` | `tests/integration` + `tests/failure` on a PostgreSQL 17 service container | includes all Milestone 1 tests |
| `stack` | full Compose stack on the runner; both Debezium connectors RUNNING; non-disruptive then disruptive stack tests; `--timeout 600` per test; logs, broker WARN/ERROR and broker GC pauses on failure | benchmarks are deliberately not run on shared runners |
| `security` | pip-audit (`requirements.lock`), gitleaks over the full history | |

Least privilege: `permissions: contents: read`, no repository secrets (CI
generates throwaway local secrets), actions pinned by commit SHA, images by
digest (except Debezium, see below). Every job has a timeout.

**History of this audit's CI runs.** Each failure was investigated from its
logs, fixed at the root, and pushed. No assertion was weakened and no test
skipped.

| Run | Commit | Result | Finding → fix |
|---|---|---|---|
| [43329288452](https://github.com/efkontoes/fleetforge/actions/runs/43329288452) | `f26908c` | 5/6 jobs green; stack failed | `_DESTROY_BROKER` from fresh admin clients; a failed wait left a thread that hung the job → bounded retry of one-shot admin calls, daemon threads |
| [43334873554](https://github.com/efkontoes/fleetforge/actions/runs/43334873554) | `ae1f123` | stack 17/19 | two test defects: seed-dependent partition coverage; `delete_records` race → fixed |
| [43336220816](https://github.com/efkontoes/fleetforge/actions/runs/43336220816) | `16eb693` | stack: 19/19 non-disruptive, 4/5 disruptive | `_UNKNOWN_PARTITION` from a fresh client after a restart → retried |
| [43338045324](https://github.com/efkontoes/fleetforge/actions/runs/43338045324) | `f98dcb7` | stack hung → 40 min timeout | no output → added faulthandler, pytest-timeout, `-v`; idle stop counted group-join time → fixed |
| [43344353785](https://github.com/efkontoes/fleetforge/actions/runs/43344353785) | `c1a6600` | stack could not start | Debezium's pinned digest was garbage-collected: upstream re-pushes `3.7.1.Final` daily → pin by release tag (documented) |
| [43344737029](https://github.com/efkontoes/fleetforge/actions/runs/43344737029) | `d46d7df` | stack 18/19 | SIGKILL test timing on a fast runner → test made speed-independent |
| [43346126778](https://github.com/efkontoes/fleetforge/actions/runs/43346126778) | `21c7500` | stack 17/19 | `describe_topics` future timed out after topic recreation → retried, fresh client per attempt |
| [43347638176](https://github.com/efkontoes/fleetforge/actions/runs/43347638176) | `2a697f7` | **all 6 jobs green**; stack: 19/19 non-disruptive + 6/6 disruptive on the runner | — |

Later commits (the consumer-liveness mitigations and this documentation) are
checked by PR #1's own checks. Their results are reported in the PR, because a
document cannot cite the CI run of the commit that contains it.

**Local evidence on the final code** (`docs/reviews/evidence/`): 182 unit,
integration and failure tests, 19 non-disruptive and 6 disruptive stack tests,
**207 passed, 0 failed, 0 skipped**. The 85 Milestone 1 test cases are included
and their files are unchanged since `c9b52b6`. After the disruptive suite the
Compose projector's consumer group was `Stable` with 1 member.

## 12. Remaining limitations

None of these are hidden by the results above. **The system is not production
ready**, and nothing here claims it is.

| Area | Limitation | Consequence / mitigation |
|---|---|---|
| Kafka availability | one broker, RF = 1, `min.insync.replicas` = 1 | losing the broker's disk loses unconsumed telemetry; production: 3 brokers, RF = 3, `min.insync.replicas` = 2 |
| Kafka security | PLAINTEXT, no authentication or ACLs | bound to 127.0.0.1 only; production: TLS + SASL/mTLS + ACLs per principal |
| Local APIs | Kafka Connect REST, schema registry, Prometheus and Grafana admin unauthenticated or with local credentials | localhost only; a local process can reconfigure connectors |
| Database owner | `fleetforge` is a superuser | anything holding `.env` owns the cluster; application roles are least-privilege and tested |
| PostgreSQL recovery | logical dumps only (`make backup-check` passes); no WAL archiving or PITR | data since the last dump is lost on disk loss |
| CDC WAL retention | `max_slot_wal_keep_size = 2GB` ≈ **20 minutes** of Debezium downtime at 5,000 telemetry events/s (measured WAL rate) | slot lost → re-snapshot of the rows still in the outbox. Alerted before and after |
| Outbox retention | rows older than N days deleted (guarded) | events deleted before a re-snapshot cannot be re-sent |
| Producer durability | in-memory queue; a SIGKILL loses unacknowledged events | a device must persist its acknowledged checkpoint and resend from it (tested) |
| Schema registry | single instance (KafkaSQL storage); an outage pauses the projector (recoverable, tested) and blocks producers that look up new ids | production: replicated registry |
| Kafka transactions | the projector's gap check assumes a non-transactional telemetry topic | written contract (runbook, ADR 0011) |
| Partitioner | librdkafka default (CRC32), which differs from Java clients' murmur2 | any non-librdkafka producer must use the same partitioner, or per-robot order breaks |
| Tail latency | sub-second spikes remain under host memory pressure (max 0.94 s, full profile) | this laptop; dedicated broker and database hosts in production |
| Capacity evidence | 10,000/s held for 60 s only; 5,000/s for 300 s | longer soaks (hours) not run: checkpoints, log segment rolls and retention deletes over hours untested |
| Slot invalidation | detected and documented, **not destructively tested** | runbook procedure not executed end to end |
| Ledger fail-closed | a recreated topic or a retention gap now **stops** the consumer | availability traded for correctness; pages via lag/staleness alerts; operator uses `ledger-reset` |
| Consumer liveness | one unexplained wedge (#9); the process stayed alive, so a restart policy alone does not recover it | detected by the external `*NotInGroup` alerts; an operator dumps stacks (SIGUSR1) and restarts. Self-healing (a liveness probe that restarts) is future work |
