# FleetForge in one page

**What it is.** A data platform for a fleet of warehouse robots that report unreliable
telemetry: duplicated, reordered, malformed, corrected and late. It keeps an exact
operational state in PostgreSQL. It also builds a lakehouse on the same Kafka log
(Spark, Iceberg, dbt, Trino), from which immutable training datasets and a tracked
baseline model come. Everything runs locally on a 3.7 GiB laptop VM; the AWS shape is
described as Terraform but never applied.

**What makes it trustworthy.** Correctness is proven, not assumed:
* An independent oracle is compared with both pipelines. 12,607 faulty deliveries end
  with 0 differences, and the latest state of every robot is identical in the oracle,
  PostgreSQL and the lakehouse.
* Incremental processing is proven equal to a full rebuild, first on 30 random
  multi-batch corpora, then on 46,696 real-Kafka records after killing Spark between
  two commits. Deliberately wrong versions of the algorithm fail the tests, which shows
  the tests are strong enough.
* Dataset releases reproduce bit for bit after newer builds, after snapshot expiry, and
  after a full disaster-recovery restore into a fresh stack.

**How it performs.**
* 5,000 events/s for 5 minutes at 388 ms p99, and 10,000 events/s absorbed.
* A million lakehouse records built on one core; a small delta over them in 124 s,
  after a measured 1.8× optimisation.
* With records arriving all the time and a 10-minute pipeline, every record reached
  Silver within 15 minutes (p95 739 s).
* Under Airflow, the pipeline recovered from six injected faults on a CI runner,
  including a killed scheduler, a killed Spark job, an object-store outage and a registry
  outage at a real schema lookup. Each of those four failures was followed by a retry,
  a repeated run and an overlapping backfill both succeeded, and reconciliation passed.

**Not yet proven.** The AWS deployment has never been applied. One producer stall,
seen twice in CI and never locally, is still open (issue #7).

**How it was built.**
* Every claim is backed by a test or an evidence file.
* Every defect found on the way (20 of them) is written down with its cause and fix,
  including two in the earlier, already-audited milestone.
* Decisions are recorded as ADRs (0001–0021).

**What I would do next.**
* A per-robot frontier index, so incremental cost stops growing with history.
* A real AWS deployment behind a budget alarm.
* Mission-aligned episodes and a real label, so the model can be more than a pipeline
  demonstration.

Start here: [README](../../README.md) ·
[architecture](../architecture/platform.md) ·
[engineering report](fleetforge-final-engineering-report.md) ·
[execution ledger](platform-execution.md)
