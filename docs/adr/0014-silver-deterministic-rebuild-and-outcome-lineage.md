# ADR 0014 — Silver: a deterministic function of its inputs, with one outcome row per source record

Status: accepted (implemented as code and unit tests; Spark execution deferred).

## Context
Silver must deduplicate, order, convert units, resolve conflicts and corrections,
and explain every exclusion. "Correct" has to be testable: two runs over the same
inputs must agree, and the Spark implementation must agree with something simpler.

## Decision
* **Silver = F(Bronze snapshot, reference data, TRANSFORM_VERSION).** No wall
  clock (the future-timestamp check uses the stored Bronze `ingested_at`), and
  "first" always means first in Bronze arrival order. The build id is a hash of
  the three inputs: an equal id means equal rows, and a rebuild with an unchanged
  id is skipped.
* **Two stages.** The row stage is one Python function (`silver.prepare`), reused
  unchanged in Spark through `mapInPandas`, so the unit-tested code is the
  executed code. It reuses Milestone 2's contract rules instead of forking them.
  The set stage is window functions in Spark, with a pure-Python reference
  (`silver.build`). Error-detail strings come from shared templates.
* **Slice 1 rebuilds Silver completely** and replaces each table in one atomic
  overwrite. Incremental processing (slice 2) must equal this rebuild.
* **Outcome lineage.** `silver.telemetry_record_outcomes` holds exactly one row per
  Bronze telemetry record: ACCEPTED, DUPLICATE or REJECTED with reason, category,
  retry eligibility and JSON details. The quarantine is a filter of it, and bytes
  come from Bronze by coordinates. Reconciliation `count(outcomes) = count(Bronze
  telemetry)` is an invariant.
* **Corrections are new versions, not updates.** Both versions stay. The latest
  correction per target wins; `superseded_by` and `is_current` make the current
  view a filter.
* **Repair vs reject.** Required fields that fail validation reject the record.
  Optional fields that are non-finite are set to NULL with a quality flag. An
  out-of-range heading is wrapped and flagged.

## Alternatives
| Option | Why not (now) |
|---|---|
| Incremental MERGE from day one | Correctness would depend on per-robot state carried across runs, with nothing to compare against. Rebuild first, then incremental tested against it |
| Python for the whole set stage (`applyInPandas` per robot) | One implementation, but every row is serialized to Python and one hot robot's history must fit in a worker. Window functions run in the JVM and spill |
| Quarantine as a separate table with copied payloads | Two sources of truth for bytes; outcome rows plus a join keep one |
| Ordering by `observed_at` | Robot clocks are untrusted (Milestone 2 contract); `(boot_number, sequence_number)` is the business order |

## Consequences
* Rebuild cost grows with Bronze. Acceptable for slice 1; slice 2 removes it.
* Without multi-table transactions, Silver tables briefly disagree during a
  build. Events are written last, and readers pair tables by `silver_build_id`.
* Changing any rule requires bumping `TRANSFORM_VERSION`. Old builds stay readable
  by snapshot until expiry, so reproducing an old dataset needs a retention policy
  (tags) for released snapshots (slice 3).
