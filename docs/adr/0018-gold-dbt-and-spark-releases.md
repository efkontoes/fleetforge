# ADR 0018: Gold is dbt on Trino for analytics, Spark for dataset releases

Status: accepted. The dbt models equal the Python reference on Trino. Spark releases
equal the reference hash, and they reproduce after newer builds and after snapshot expiry.

## Context
Gold has two kinds of products:
* daily analytics, which are aggregations people query;
* training-candidate datasets, which must be immutable and reproducible bit for bit.

Silver is already built by Spark. The question is where Gold logic lives without
writing Silver twice.

## Decision
* **Split of duties.** Spark builds Bronze and Silver. dbt (dbt-trino) builds the
  analytical Gold tables and runs the data tests. A Spark job builds dataset releases.
  Airflow only orchestrates. dbt never re-derives Silver; it reads Silver tables at
  the snapshots of one build from `silver.build_log` (`FOR VERSION AS OF`).
* **One schema source.** Gold columns and types are declared in
  `lakehouse/tables.py` (with `writer="dbt"`). dbt model contracts repeat them, and a
  unit test fails if the two differ.
* **Finality is a policy.** A daily row is final when it is older than the 14-day
  correction window and its robot has no open retry-eligible record. Final rows are
  frozen by the incremental model. Recomputing them needs an explicit reopen (all
  days, or a date range).
* **Releases are pinned and tagged.** A release reads one build's events snapshot,
  writes its rows, tags every input snapshot `release-<id>`, and publishes by appending
  the release row last. The content hash is SHA-256 over the canonical rows in
  `observation_id` order. One Python function computes it, both for the Spark job
  (rows streamed to the driver) and for the reference, so the two cannot drift.
* **Splits are hashes.** `split` is `sha256("seed:boot_id")` and `split_robot` is
  `sha256("seed:robot_id")`, each mapped to 70/15/15. No episode, and for the strict
  split no robot, appears in two splits.

## Alternatives
| Option | Why not |
|---|---|
| Gold in Spark only | Works, but analysts lose SQL models, docs and tests; dbt gives those for little memory |
| Gold in dbt only, releases included | Trino cannot create Iceberg tags, and the hash would be computed in SQL with engine-specific float formatting |
| Recompute final days on every run | Simple, but "final" would mean nothing; restatements must be deliberate |
| Random splits | Not reproducible from the release row alone, and they leak episodes across splits |

## Consequences
* The analytical models rescan the current Silver events of the pinned build on each
  run, which is cheap at our size. Frozen rows bound what changes, not what is read.
* Gold daily rows of a final day keep the build id that computed them. Readers can see
  that a frozen row predates the newest build.
* A release can be reproduced only by code with the same `FEATURE_SCHEMA_VERSION`; a
  change of feature definitions is a new version, never an edit.
