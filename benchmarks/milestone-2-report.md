# Milestone 2 benchmark report

Every number here was measured on the machine below. Raw JSON for each experiment
is in `benchmarks/results/m2-*.json`; file names are given next to each table.
Estimates are labelled as such. This version was rewritten during the final audit
(2026-11-13). Earlier figures that did not survive re-measurement are kept at the
end with the reason they were superseded.

## Environment

| | |
|---|---|
| Host | Windows 11 laptop, Intel i5-10210U (4 cores / 8 threads, 1.6 GHz base), 7.8 GiB RAM |
| WSL2 | Ubuntu 24.04, 8 vCPUs, **3.7 GiB RAM** (deliberately not raised), 1 GiB swap; Docker with cgroup v1 |
| Storage | WSL2 ext4 virtual disk, Docker named volumes |
| PostgreSQL | 17.11, `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, `checkpoint_timeout=5min`, 768 MiB limit |
| Kafka | 4.3.1 KRaft, 1 broker, RF=1, heap 384 MiB, **G1** (after the audit fix; Serial before), 768 MiB limit and no swap |
| Clients | Python 3.12, confluent-kafka 2.16.1 (librdkafka 2.16.0), fastavro 1.13.1, psycopg 3.3 |
| Profiles | **core** (`make up-bench`: PostgreSQL, Kafka, registry, API) for all throughput numbers unless stated. **full** (+ Debezium, mission consumer, projector container, Prometheus, Grafana, exporters) where labelled: with the benchmark processes it fills the 1 GiB of swap |

## Methodology

| Measurement | Starts | Ends |
|---|---|---|
| HTTP latency | client sends request | HTTP response (DB committed) |
| Drain throughput | first projector batch starts on a pre-loaded backlog | last batch committed |
| **Event→state latency (traced)** | the event's **scheduled** send time under the offered load | its batch's COMMIT returned (visible to every new snapshot) |
| Durable lag | — | Σ Kafka high watermarks − Σ PostgreSQL ledger `next_offset`, sampled every second |
| WAL | `pg_current_wal_lsn()` delta; `pg_stat_wal` records and full-page images; a `CHECKPOINT` before each drain window |

Rules every run follows:

* **Fresh robots, a dedicated topic and a dedicated consumer group** per run.
  Every run is checked against the in-memory oracle afterwards: final state,
  `state_version` per robot, and the exact set of quarantine rows for the topic.
  All runs in this report match.
* **One clock.** `benchmarks/m2_traced.py` timestamps every sampled event
  (offsets ≡ 0 mod 4, or mod 8 for the 5-minute run, chosen identically by
  producer and projector) with `time.monotonic_ns()`. CLOCK_MONOTONIC is
  system-wide on Linux, so producer, projector and harness processes share a
  timeline. Kafka's millisecond wall-clock timestamps are not used.
* **No coordinated omission.** Latency starts at the scheduled send time, so a
  producer that falls behind is charged for it (`producer_lateness`).
* **Bounded lag is judged over time**, by comparing mean durable lag in the first
  and second halves of the producing window (after a 10 % warm-up) and checking
  that the backlog drains when production stops. A p99 threshold is not used.
* Producers generate their fleets lazily (constant memory), so runs can last
  minutes. Workload faults: 1 % duplicates, 1 % reorders, 0.5 % invalid
  battery, 0.5 % malformed, 0.05 % reboots.
* Peak and sustained throughput are reported separately:
  * **peak** = the highest 1-second processing rate, or the backlog drain rate;
  * **sustained** = the highest *offered* rate at which lag stays bounded and
    drains.

Reproduce:
```bash
make env && make up-bench                       # core profile
API_PROCESSES=4 docker compose --profile core up -d --wait api
python benchmarks/m2_benchmark.py http  --robots 200 --ticks 75 --processes 4 --threads 8
API_PROCESSES=2 docker compose --profile core up -d --wait api
python benchmarks/m2_benchmark.py drain --robots 200 --ticks 75 --modes per_event:1 loop:500 set:500 set_copy:500
python benchmarks/m2_benchmark.py drain --robots 300 --ticks 75 --consumers 3 --modes set:500
for r in 1000 2500 7500 10000; do python benchmarks/m2_traced.py --rate $r --seconds 60 --label sustained-core-$r-60s; done
python benchmarks/m2_traced.py --rate 5000 --seconds 300 --trace-every 8 --label sustained-core-5000-300s
python benchmarks/m2_traced.py --rate 12500 --seconds 60 --producers 3 --label sustained-core-12500-60s
python benchmarks/projector_gc.py               # decode/validate CPU and GC, no infrastructure
```

## 1. Architectures A–D on the same workload (15,000 events, 1 consumer)

`m2-20261113T003729Z-http.json`, `m2-20261113T004449Z-drain.json`,
`m2-20261113T005341Z-drain.json` (two drain sweeps; ranges are across the two).

| Arch | What | Events/s | PG commits/event | WAL records/event ¹ |
|---|---|---:|---:|---:|
| **A** | HTTP → PostgreSQL, 1 event/tx (4 API processes, 32 client threads) | **330** | 2.0 ² | — |
| **B** | Kafka → projector, 1 event/tx (`per_event`) | **51–55** | 1.0 | 6.13 |
| **C** | Kafka → projector, 1 tx/batch, 1 statement/event (`loop:500`) | **896–1,128** | 0.002 | 3.33 |
| **D** | Kafka → projector, set-based (`set:500`) | **7,680–8,591** | 0.002 | **1.06** |
| D′ | set-based, rows staged with COPY (`set_copy:500`) | 8,106–8,891 | 0.002 | 1.52 |

¹ From `pg_stat_wal` after a forced `CHECKPOINT`. WAL *bytes* per event are not
comparable over 15,000-event windows: about 0.05 full-page images per event
(~400 B of 8 KB page images) dominate every mode equally. The sustained runs
(§3) give the bytes that matter operationally.
² `pool_pre_ping` adds an autocommit `SELECT 1` per pool checkout.

**Why Kafka alone was slower than HTTP (A → B, 6× slower).** HTTP had 32
concurrent writers whose commits PostgreSQL flushes together (group commit).
One consumer committing per event is a single serial writer: decode, ledger
lock and advance, lookups, upsert and a ~4–6 ms WAL flush, for every event.
Kafka adds buffering, replay and decoupling. Speed comes only from what the
consumer does with them.

**Why set-based batching gave the largest gain.**
* B → C (one transaction per 500 events): 17–21×, one WAL flush per batch
  instead of per event.
* C → D (constant number of statements per batch, one row write per robot
  per batch): a further 7–8×, with 3× fewer WAL records per event. D is then
  CPU-bound in Python, which is why the projector fix (§4) raised D from the
  earlier 5,224 to 7,680–8,591 events/s.

## 2. Consumer concurrency (set:500, 22,500 events)

`m2-20261113T005458Z-drain.json` (1 consumer), `m2-20261113T005551Z-drain.json`
(3 consumers). Consumers join before the backlog arrives.

| Consumers | Drain events/s |
|---:|---:|
| 1 | 5,928 ³ |
| 3 | **11,771** (was 9,280 before the projector fix) |

³ On this shorter workload the producers' burst overlaps the first part of the
drain. The §1 workload measures 1-consumer capacity more cleanly.

## 3. Sustained ingestion

`m2-…-traced-sustained-core-{1000,2500,7500,10000}-60s.json`,
`m2-20261113T002429Z-traced-sustained-core-5000-300s.json`,
`m2-…-traced-sustained-core-{12500,15000}-60s.json`.
Core profile, 3 projector processes, `set:500`, 50 ms flush, 1,000 robots,
2 producer processes (3 above 10,000/s).

| Offered | Duration | Achieved | Processed | Event→state p50 / p95 / p99 / max | Lag mean 1st → 2nd half | Max lag | Drain after stop | Verdict |
|---:|---:|---:|---:|---|---:|---:|---:|---|
| 1,000 | 61 s | 999 | 1,000 | 72 / 118 / 190 / 339 ms | 57 → 56 | 148 | 0.3 s | stable |
| 2,500 | 61 s | 2,499 | 2,498 | 73 / 112 / 127 / 209 ms | 136 → 147 | 274 | 0.2 s | stable |
| **5,000** | **303 s** | **4,998** | **5,000** | **86 / 179 / 350 / 686 ms** | **394 → 373** | 2,019 | 0.2 s | **stable** |
| 7,500 | 61 s | 7,493 | 7,491 | 108 / 187 / 262 / 418 ms | 602 → 862 | 2,759 | 0.0 s | stable (lag returned to < 1,000) |
| **10,000** | 61 s | 9,992 | 9,962 | 147 / 247 / 470 / 773 ms | 1,431 → 1,386 | 5,089 | 1.0 s | **stable** |
| 12,500 | 61 s | 12,462 | 12,227 | 202 / — / 1,650 / 1,864 ms | 1,715 → **11,984** | 19,190 | 1.8 s | **not sustainable**: lag grows |
| 15,000 | 68 s | 13,336 | 9,920 | 9.7 s / — / 23.6 s / 24.2 s | 35,771 → 144,529 | 207,820 | 12.6 s | **overloaded**: the producers fall 8 s behind schedule; host load 9.3 |

* **Sustained throughput: 10,000 events/s** is the highest tested rate where lag
  stays bounded (60 s run). **5,000 events/s was held for 5 minutes**
  (1,515,049 events). Between 10,000 and 12,500 the 4-core host saturates: at
  12,500 every batch is full (493 of 500) and host load is 6.3.
* **Peak throughput:** 13,201 events/s in the best 1-second window (10,000/s
  run), and 11,771 events/s backlog drain with 3 consumers (§2).
* **Every event accounted for**, for example at 5,000/s × 303 s: generated
  1,515,049 = acknowledged = consumed = high watermark = durably applied (ledger).
  Quarantine rows equal the oracle's exactly.

**The 5-minute run, hop by hop** (189,383 sampled events, ms):

| Segment | p50 | p95 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| producer lateness vs schedule | −0.3 | 0.3 | 14 | 343 | 522 |
| produce() → broker ack | 13 | 22 | 23 | 47 | 526 |
| produce() → projector receipt | 55 | 103 | 187 | 300 | 396 |
| receipt → batch start | 0.4 | 1.1 | 9.9 | 19 | 148 |
| decode + validate (per batch) | 7.0 | 19 | 40 | 75 | 99 |
| SQL inside the transaction | 17 | 38 | 65 | 128 | 276 |
| COMMIT | 4.2 | 10 | 90 | 257 | 295 |
| **scheduled → committed** | **86** | **179** | **350** | **520** | **686** |

Resources in that run:
* CPU: projector 0.27 cores per process, producer 0.49 per process (means);
  Kafka 0.43 cores, PostgreSQL 0.30 cores (docker samples).
* PostgreSQL: **72 commits/s**, mean batch 142 events.
* WAL: **335 B/event, 1.7 MB/s**. Batches are small when consumers keep up,
  so the per-transaction overhead and page images are not amortised over 500
  events. Consequence: the 2 GB `max_slot_wal_keep_size` lasts only about
  **20 minutes** of Debezium downtime at this rate (runbook).
* Memory: container peaks Kafka 495 MiB, PostgreSQL 416 MiB, registry 223 MiB,
  API 149 MiB; each projector process peaked at 113 MiB RSS. Minimum
  `MemAvailable` on the host was 1,172 MiB.

## 4. The latency tail: investigation and fix

Details, method and per-run evidence are in the audit
(`docs/reviews/milestone-2-final-audit.md` §8). In short:

**Root causes, by evidence:**
1. **Broker stop-the-world GC.** With a 768 MiB container the JVM picked the
   **Serial** collector. Its Full GCs froze the broker:
   * **2,104.13 ms at 18:53:23**, inside the original 5,000/s run whose p99 was
     **2,104.56 ms**;
   * **11,772.8 ms** during the first traced run of this audit.

   That pause used 2.9 s of CPU and 11.8 s of wall time: the heap was being
   paged in from a full swap. Excerpt: `m2-kafka-broker-gc-serial-excerpt.txt`
   (7 Full GCs of 0.5–11.8 s in one day).
2. **Projector CPU and queueing under contention.** Each projector process
   spent ~130 µs CPU per event in decode/validate, about 25 % of it in
   confluent-kafka's per-message data-contract machinery we do not use. On
   top of that came a 60–160 ms gen-2 GC every ~2.5 s. On a host already
   overcommitted by the full profile, records waited in Kafka for a busy
   consumer: `produce→receipt` dominated every remaining episode, with durable
   lag up to 14,207.
3. **Cold start** (minor). The first 2–4 s of a fresh projector (connections,
   schema fetch, cache fill, one start-up GC) produced one 1.9 s episode in
   five core-profile runs. A long-running service pays this once per restart.

**Fixes** (smallest corrections; no queues made larger, nothing dropped):
* broker on G1 with Kafka's documented settings, and no swap for its container;
* the projector decodes with fastavro directly (proved byte-identical to
  AvroDeserializer) and builds audit payloads only for rejected records:
  **132 → 36 µs CPU/event** (`benchmarks/projector_gc.py`);
* `gc.freeze()` of the start-up heap: gen-2 pause max **39 → 0.8 ms** in the
  micro-benchmark.

**Before and after, 5,000 events/s, 60 s runs:**

| Series | Broker GC | Projector | Profile | Runs | p50 | p99 | max | Runs with a > 1 s episode |
|---|---|---|---|---:|---:|---:|---:|---:|
| `prefix-serialgc-5000` | Serial | before | full | 5 | 138–532 ms | 557–3,120 ms | 0.80–3.45 s | **4** |
| `full-allfixes-5000` | G1 | after | full | 5 | 82–103 ms | 200–621 ms | 0.34–0.94 s | **0** |
| `core-g1broker-oldprojector-5000` ⁴ | G1 | before | core | 5 | 138–153 ms | 334–1,617 ms | 0.46–1.94 s | 1 (cold start) |
| `core-g1broker-projfix-5000` ⁴ | G1 | after | core | 3 | 81–82 ms | 206–482 ms | 0.34–0.94 s | 0 |

⁴ These two series were labelled "serialgc" when they ran. The broker had in
fact already been recreated with the G1 fix (container start 23:34:12Z, GC log
"Using G1"). The files were relabelled, with a note inside each.

Since the fix, the broker's G1 log over all post-fix load (390 pauses) shows a
maximum pause of 393 ms (3 pauses over 100 ms, all under swap pressure), against
Serial Full GCs of 2–11.8 s. **A Serial/G1 A/B within this audit was not run:**
Serial Full GCs occurred about every 20–60 minutes of load, so a 15-minute A/B
would most likely show nothing. The attribution rests on the GC-log timestamps
and on the mechanism, not on a controlled A/B. Two causes of tails remain:
memory pressure on this laptop, and cold starts.

## 5. Recovery (from the failure tests)

| Scenario | Measured |
|---|---|
| PostgreSQL stopped while Kafka kept accepting | no records quarantined, no offsets advanced; backlog drained after restart, state = oracle (`junit-stack-disruptive.xml`) |
| Kafka stopped 8 s during publishing | no acknowledgement during the outage; all messages acknowledged exactly once afterwards |
| Kafka stopped 10 s with `message.timeout.ms=3000` | queued events reported as **delivery failures** (not silent); resent from the checkpoint; state = oracle |
| Producer SIGKILLed | acknowledged ≤ in Kafka < produced; resend from the last contiguous acknowledged index; state = oracle |
| Projector crash after DB commit | resume from the ledger: 0 records re-read |
| Kafka Connect SIGKILLed during mission traffic | stall visible end to end after 17–25 s (oldest undelivered change > 15 s; two runs); all missions downstream 27–46 s after restart (four runs; last: 163 missions) |
| Backlog drain rate | 11,771 events/s with 3 consumers |

## 6. Bottleneck analysis

1. **Single-row durable commits** (B): one ~4–6 ms WAL flush per event.
   Removed by batching.
2. **Round trips per event** (C): removed by set-based SQL.
3. **Python CPU in the projector** (D): ~0.15 cores per 1,000 events/s for all
   projector work. Measured: 0.27 cores per process at 1,670 events/s each
   (5,000/s over 3 processes), and 0.48 cores at 3,330 events/s each (10,000/s).
   Decode and validate alone are now 36 µs/event (0.036 cores per 1,000/s). The
   rest is consuming, building SQL parameters and the driver.
4. **The shared 4-core host.** Above ~10,000 events/s, producers, broker,
   projectors and PostgreSQL saturate the same cores (load 6.3 at 12,500/s).
   PostgreSQL ran at **54–86 commits/s** in every sustained run: it is not the
   bottleneck.

## 7. Capacity projection (estimate, not measured)

At ~0.15 cores per 1,000 events/s, one projector process on a dedicated core
would top out around 6,000 events/s. Six processes, one per partition, on six
dedicated cores: roughly **30,000–35,000 events/s** before more partitions are
needed. PostgreSQL would then commit ~60–100 batches/s at batch 500, far from
its limit. At that point the single broker with RF = 1 on one disk is the next
constraint. Untested assumptions: RF = 3 acknowledgement
latency, a managed database's network round trips, larger fleets per batch.

## 8. Decisions taken from these numbers

* Projector mode `set`, batch 500. Flush interval 50 ms in benchmarks; 200 ms
  in deployment trades latency for fewer transactions.
* Keep `unnest()`. COPY is within noise at 500 rows, writes more WAL records,
  and adds a temporary table.
* The broker must not run on Serial GC, and its heap must not swap.
* Size `max_slot_wal_keep_size` from the measured WAL rate (1.7 MB/s at
  5,000/s) × the longest CDC outage to tolerate.

## Superseded figures (kept for transparency)

| Earlier claim | Status | Why |
|---|---|---|
| 5,000/s sustained with "typical p99 ≈ 250 ms; one run in four 2.1 s; the stall was producer→broker" | **cause corrected** | The 2.1 s was a broker Full GC (Serial collector), matched to the millisecond in the GC log. "Producer→broker" named the symptom. |
| `kept_up = p99 < 2 s` | **method replaced** | Not a stability test. Replaced by durable lag over time. |
| Event→state measured from Kafka CreateTime | **method replaced** | Coordinated omission: a late producer hid its own delay. Now measured from the scheduled send time. |
| "Drop the first 10 % of samples" warm-up trim | **bug** | It ran on the concatenation of per-consumer lists, so it trimmed only the first consumer's samples. |
| D (set:500) 5,224 events/s; 3 consumers 9,280 | **superseded** | Re-measured after the projector CPU fix: 7,680–8,591 and 11,771. |
| WAL 653 → 80 B/event (8×) | **not reproduced** | Short windows are dominated by full-page images, depending on where the window falls relative to a checkpoint. Re-measured as WAL *records* per event: 6.13 → 1.06 (6×). |
| Up to ~8,800 events/s "still keeping up" | **superseded** | Judged by p99 only. With lag-over-time, 10,000/s is the highest stable tested rate after the fixes. |
| Files `…184248Z-drain-consumers2-SUPERSEDED…`, `…184306Z-drain-consumers3-SUPERSEDED…`, `…185054Z-sustained-c3-SUPERSEDED…` | superseded earlier | See the file names: late-joining consumers; producer timer including fork/pickle. |
| `m2-20261112T185714Z-sustained-c3-final.json`, `…c3-5k-rep{1,2,3}.json`, the 2026-11-12 drain sweeps and HTTP run | **historical** | Kept unchanged as raw data; they ran on the Serial-GC broker and the earlier projector. |
