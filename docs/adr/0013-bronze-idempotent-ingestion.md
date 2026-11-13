# ADR 0013 — Bronze ingestion: bounded Kafka batches, checkpoint in the Iceberg commit, insert-if-absent

Status: accepted; implemented and proven on Spark 4.1.4, Iceberg 1.12.0, Garage and Kafka
(spec §8).

## Context
Bronze must hold every Kafka record exactly once per identity, survive crashes at
any point, and never silently skip records. Milestone 2 solved the same problem
for PostgreSQL: the offset ledger is written in the same transaction as the state
(ADR 0006), bound to the topic incarnation, and fails closed (ADR 0011). Kafka and
Iceberg cannot share a transaction.

## Decision
1. **Identity** `(kafka_topic_id, kafka_partition, kafka_offset)`. The topic id
   distinguishes incarnations of a topic name.
2. **Bounded batch reads.** Each run plans exact offset ranges and reads them with
   Spark's Kafka *batch* source (`assign`, `startingOffsets`, `endingOffsets`,
   `failOnDataLoss=true`). It then verifies count, minimum and maximum per
   partition (`verify_batch`), and checks that the topic id did not change during
   the read.
3. **Checkpoint in the commit.** The append carries `fleetforge.bronze.checkpoint`
   (next offset per topic and partition, all topics copied forward), the ranges and
   the run id as Iceberg snapshot-summary properties. Data and checkpoint are one
   atomic commit. Readers walk the snapshot ancestry to the newest snapshot with a
   checkpoint (compaction adds snapshots without one), and merge it with a
   checkpoint derived from the data (`merge_checkpoints`).
4. **Insert-if-absent.** The batch is anti-joined with Bronze rows in the same
   ranges. The predicate is a literal (`range_predicate_sql`), so Iceberg prunes by
   partition and by file statistics.
5. **Fail closed** (`plan_topic`): retention gap, recreated topic, vanished
   partition, checkpoint beyond the log end → `BronzePositionError`. The first two
   have explicit, audited overrides (`--accept-gaps`,
   `--accept-new-incarnation`). The third has none.
6. **One writer**: a PostgreSQL advisory lock in the catalog database for the
   whole run, plus a post-commit check that the new snapshot's parent is the
   snapshot the plan was based on.

## Alternatives
| Option | Why not |
|---|---|
| Spark Structured Streaming with an Iceberg sink | Offsets live in a checkpoint *directory*. Losing it replays from the configured start, and only Iceberg's epoch check stays idempotent. Planned ranges are explicit, auditable and unit-testable. Streaming is a reasonable slice-4 option once the semantics are pinned down by tests |
| `MERGE INTO` for insert-if-absent | Equivalent in effect (an insert-only MERGE is planned as an anti-join plus append). The explicit form keeps the pruning predicate visible and testable |
| Checkpoint only in Kafka consumer-group offsets | Not atomic with the Iceberg commit; the same lesson as ADR 0006 |
| Checkpoint in a table property | Separate commit from the data, so not atomic |

## Consequences
* A crash or unknown commit outcome is resolved by re-reading the table: there is
  no second system of record to reconcile.
* An append whose rows were all present must still advance the checkpoint. Whether
  Iceberg commits an empty append was measured: it does
  (`empty_append_created_snapshot=True`).
  `merge_checkpoints` makes the design correct either way.
* Records deleted by compaction before ingestion (compacted CDC topic) are
  unrecoverable: Bronze guarantees latest-per-key for such topics, not full history
  (gap G6).
* The data-derived checkpoint scans offsets per topic. That is fine locally; at
  scale it should be answered from Iceberg file statistics (slice 4).
