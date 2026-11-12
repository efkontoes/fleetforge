# ADR 0009 — Error taxonomy, quarantine table, controlled replay

Status: accepted (Milestone 2)

## Decision
* Three categories with different actions (`domain/errors.py`):
  * PERMANENT_DATA → quarantine and continue;
  * RECOVERABLE → retry in place while holding offsets;
  * ORDERING → record or count.
  Some permanent errors are `retry_eligible` (unknown robot/boot/firmware).
* The quarantine is `fleet.telemetry_rejections` extended with source
  coordinates, raw bytes, category, retry eligibility, status
  (`QUARANTINED/RECORDED/REPLAYED/RESOLVED/DISCARDED`) and consumer version, plus
  the view `fleet.telemetry_quarantine`. It is written in the same transaction as
  the offset ledger. There is no DLQ topic: that would be a second, non-atomic write.
* `fleet-replay` handles single events, offset ranges, eligible quarantine and
  corrections. It runs the live `BatchProcessor` (bypassing the ledger),
  `--dry-run` rolls back, quarantine status changes commit with the state change,
  and every run is audited in `fleet.replay_runs`.
* A correction is a new event (uuid5 of the original + attempt) with
  `correction_of`. It cannot overwrite newer state, and a second, different
  correction for the same position is a SEQUENCE_CONFLICT.

## Consequences
* Audit rows are deduplicated by `(dedup_key, reason)`. The dedup key is the
  `event_id` when one can be read, otherwise a SHA-256 of the raw bytes. Byte-
  identical undecodable payloads therefore produce one row however often they are
  delivered: redeliveries add no information. The Prometheus counter
  `fleetforge_projector_quarantined_total` still counts every delivery.
Logs carry reason codes, event ids and offsets but never payloads; payloads live
only in the access-controlled quarantine table.
