# ADR 0017: Incremental Silver must equal a full rebuild

Status: accepted.
* The Python reference matches a full rebuild in 30 randomized multi-batch scenarios,
  and three deliberately wrong closures each fail some of them.
* On Spark, incremental builds match a Spark full rebuild and the Python reference over
  a three-batch random corpus.

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
* **Recompute the closure, not the table.** A record can only affect:
  * rows that share its `event_id` (duplicates, conflicts) or a correction link;
  * rows of its robot from its own boot on.

  Earlier boots have smaller ordering keys, so they can never make a newer row late,
  and a new arrival never changes what earlier arrivals saw. The build iterates to a
  fixed point: a first boot per robot, and the link keys of that scope. Rows that were
  prepared again count from boot 0, because their outcome may change what any later
  arrival saw. A rejected prepared row keeps the `correction_of` it could read. Without
  it, a correction that a firmware change turns into a rejection would drop its link,
  and its old target would keep a stale `superseded_by` (found by Gate A, defect D9). Rows without a robot or boot are included by key or coordinates. Only
  that scope goes through the set stage, and the events of each robot are swapped from
  its first recomputed boot.
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
  `TRANSFORM_VERSION` changed, or when it is requested with `--full`. A build with no
  new telemetry and unchanged reference and archive is a no-op, even when Bronze
  committed an empty snapshot (a replay does that to record its checkpoint).
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
* The incremental build's cost is proportional to the rows from each touched robot's
  current boot onwards, not to all of Bronze. In Gate A's first run, without the boot
  bound, a 62-row batch rebuilt 45,442 rows, because every robot reports in every
  batch. The boot bound was added as the measured fix, and the second run's numbers
  are in the execution ledger. A robot that never reboots still accumulates one long
  boot; bounding that needs a per-robot summary row and is future work.
* Old builds stay readable by the snapshot ids in the build log until snapshots expire.
  Releases pin theirs with tags.
* Spark 4.1 cannot plan a MERGE whose source is a temp view that still reads an Iceberg
  table. Every MERGE source is computed first with `localCheckpoint()`.
