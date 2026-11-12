# ADR 0004 — Three validation layers; every rejection audited with its original payload

Status: accepted (Milestone 1)

## Context
"Clean data" means different things to different consumers. FastAPI's default
422 for an invalid body never reaches the application, so it cannot be audited.

## Decision
* **Structural** checks use a strict Pydantic contract (`extra=forbid`, no
  coercion, no NaN/Infinity, timezone-aware timestamps).
  **Semantic** checks are pure domain functions (battery 0–100, `observed_at` no
  more than 5 min ahead). **Referential** checks are done in the database
  (robot and boot known).
* The telemetry endpoint reads the raw body and validates it itself, so every
  rejection, including malformed JSON, is stored in `telemetry_rejections` with
  a machine-readable `reason_code`, an `errors[]` list and the original payload.
* Audit inserts are idempotent: `dedup_key` is the `event_id` when it parses,
  otherwise the SHA-256 of the raw bytes, with `UNIQUE (dedup_key, reason_code)`.
* The database repeats the critical value rules as `CHECK` constraints.

## Consequences
* Unknown fields are rejected, so adding a field means a new schema version.
  That is deliberate: v1 consumers are protected from silent drift.
* Duplicates are not rejections; they are counted in metrics only.
* Rejections are kept in PostgreSQL in M1. In M2 they move to
  `telemetry.invalid.v1` and the lakehouse quarantine.
* The firmware contract table exists, but M1 only accepts the canonical units.
  Unit conversion ("canonicalize when known") is deferred to M2.
