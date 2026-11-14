# Lakehouse backup and disaster recovery

`scripts/lakehouse_dr.sh` runs both halves. The restore is tested for real, into a
separate and disposable stack (`fleetforge-dr`). Restored data is queried and
reconciled; the presence of the files alone is not taken as proof.

## What is backed up
| Part | How | Why it is enough |
|---|---|---|
| Iceberg catalog (both catalog databases) | `pg_dump -Fc` | the catalog only holds a pointer to each table's current metadata file |
| Object store (the whole `lakehouse` bucket) | copied through the S3 API (`dr_check.py copy-out`) | data, manifests and metadata files are immutable; the copy does not depend on Garage's internal layout |
| Expected state | row count and current snapshot id of every table, recorded just before the copy | the restore is compared against it |

Kafka is not backed up here. Milestone 2 keeps telemetry for 7 days, and that window is
what makes the recovery point below so small.

## Restore test (measured 2026-11-14, Gate A data: 15 tables, 42 MB, 372 objects)
1. Stop the live lakehouse; its volumes are kept.
2. Start `fleetforge-dr` with empty volumes, `pg_restore` both catalogs, and copy the
   objects back.
3. Verify:
   * every table has the same rows and the same current snapshot id;
   * every dataset release reproduces (recorded, stored and recomputed hashes agree);
   * all reconciliation checks pass.
4. Delete the disposable stack and start the live one again.

| Measure | Result |
|---|---|
| Restore (catalog + objects) | 24 s |
| Restore plus full verification | 83 s |
| Tables identical (rows and snapshot id) | 15 of 15 |
| Releases reproduced | 2 of 2 |
| Reconciliation checks passed | 12 of 12 |

## Objectives
* **RTO** for the lakehouse is minutes. The restore moved about 1.75 MB per second on this
  machine, plus about a minute of verification. Restore time grows with bucket size, so
  for production it should be measured with S3's batch copy, not extrapolated.
* **RPO**:
  * Bronze loses nothing written in the last 7 days. A restored Bronze resumes from its
    restored checkpoint and re-reads the newer records from Kafka (`bronze_ingest`,
    insert-if-absent).
  * Silver and Gold are rebuilt from Bronze.
  * Data older than the Kafka retention at the time of the restore survives only if the
    backup holds it, so backups must run more often than every 7 days. Daily is the
    default.
* Releases survive by tag. A restored release reproduces bit for bit, which the test
  above checks.

## Consistency rule
Take the catalog dump first and copy the objects second, while no maintenance job runs
(the maintenance lease). Writers may keep running: files they add later are orphans in
the backup, which is harmless. Snapshot expiry, however, could delete a file the dumped
catalog still references, so the backup and maintenance must not overlap.
