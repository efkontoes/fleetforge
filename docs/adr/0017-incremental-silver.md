# ADR 0017: Incremental Silver must equal a full rebuild

Status: accepted. The Python reference matches a full rebuild in 30 randomized
multi-batch scenarios. On Spark, incremental builds match a Spark full rebuild and the
Python reference over a three-batch random corpus.

## Context
Slice 1 rebuilt Silver from all of Bronze every time (ADR 0014), so the cost grew with
history instead of with change. The requirement is S_{t+1} = U(S_t, ΔB, ΔR, V) ≡ F(B_{t+1}, R_{t+1}, V):
an update from the previous state must give the same rows as a rebuild over the same inputs.

## Decision
* **Persist the row stage.** `silver.telemetry_prepared` keeps the output of
  `silver.prepare` for every Bronze telemetry record. A record is decoded, converted and
  validated once, and again only when its inputs change.
* **Checkpoint in a build log.** `silver.build_log` gets one row per published build,
  holding the Bronze snapshot, the largest Bronze `ingested_at`, the reference and
  archive fingerprints, the transform version, and the snapshot ids of the Silver tables
  it wrote. The row is appended last, so a build without a log row never happened. The
  next build reads only Bronze rows newer than the checkpoint (ΔB).
* **Re-prepare what changed underneath.** Rows are prepared again if:
  * their robot's model changed (a late `RobotRegistered`);
  * their model's firmware contract changed units (ΔR);
  * their schema id got a different archived answer, typically NOT_FOUND turning into
    FOUND (ADR 0016).
* **Recompute the closure, not the table.** A record can only affect rows that share its
  robot (slots, order), its `event_id` (duplicates, conflicts) or a correction link. The
  build takes the changed rows and iterates to a fixed point over those keys. Robotless
  rows (malformed, unresolved) are included by event id or by coordinates. Only that
  scope goes through the set stage.
* **Write order and atomicity.** The writes go in this order:
  1. Prepared rows MERGE by Kafka coordinates.
  2. Outcomes MERGE by coordinates.
  3. Events of the scoped robots are swapped in one row-level MERGE (`WHEN NOT MATCHED
     BY SOURCE AND robot_id IN (...) THEN DELETE`). The swap refuses to run if the
     events table moved since the build read it.
  4. Reference tables are written.
  5. The build log row is appended.

  Every step is idempotent, so a crash at any point is repaired by running the build
  again from the same checkpoint.
* **Fall back to full.** A full rebuild runs when there is no base build, when
  `TRANSFORM_VERSION` changed, or when it is requested with `--full`. Unchanged inputs
  are a no-op.
* **One writer.** Silver has a single scheduled writer (Airflow, `max_active_runs=1`).
  A conflicting commit, from Iceberg's validation or our own snapshot check, is retried
  from the checkpoint up to three times.

## Alternatives
| Option | Why not |
|---|---|
| Keep rebuilding everything | Simple, but the cost is proportional to history; it stays available as `--full` and as the oracle |
| Per-robot state rows (last slot, last sequence) | Faster for append-only streams, but corrections and late registrations would need special cases; the closure handles them with the same code as a rebuild |
| `overwrite(robot_id IN ...)` for events | Iceberg can only delete whole files by filter, and event files hold many robots (partitioned by day and robot bucket) |
| Delete then append | Two commits; a reader between them sees robots with no events |

## Consequences
* The incremental build's cost is proportional to the scoped robots' prepared history,
  not to all of Bronze. When every robot reports in every batch, the scope approaches all
  prepared rows. That is still cheaper than a rebuild because nothing is decoded again,
  but it is not O(ΔB). Seeding the set stage with per-key state would fix this and is
  left as future work.
* Old builds stay readable by the snapshot ids in the build log until snapshots expire.
  Releases pin theirs with tags.
* Spark 4.1 cannot plan a MERGE whose source is a temp view that still reads an Iceberg
  table. Every MERGE source is computed first with `localCheckpoint()`.
