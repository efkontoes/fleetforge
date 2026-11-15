# Fault matrix

Every fault the platform claims to survive, with the test that injects it and where the
evidence is. "Expected" is the behaviour the design promises. A row is filled in only
from a run that actually happened.

## Lakehouse (Milestones 3-6)

L15-L20 come from Gate B, CI run 43704310730 (`evidence/ci-gate-b/gate-b.json`); reconciliation passed 11/11 after all of them.

| # | Fault | Injected by | Expected | Result |
|---|---|---|---|---|
| L1 | Spark killed (SIGKILL) between two Silver commits | Gate A `kill-silver` | next build repairs it; Silver equals a full rebuild | passed: incremental equals full and the Python reference on 46,696 records |
| L2 | Crash after Bronze read, before commit | `test_bronze_crash_before_commit_restarts_without_loss_or_duplicates` | nothing visible; the restart ingests once | passed |
| L3 | Crash after the prepared MERGE | `test_spark_incremental_recovers_from_a_crash_after_the_prepared_merge` | rerun from the checkpoint repairs it | passed |
| L4 | Stale writer commits to events mid-build | `test_a_concurrent_events_writer_fails_validation_and_the_build_is_retried` | build detects it and retries | passed (after D5) |
| L5 | Replay of every Kafka offset | Gate A `bronze --replay` | 0 new Bronze rows; Silver no-op | passed (after D7) |
| L6 | Rule change | `test_a_new_rule_version_forces_a_full_rebuild` | full rebuild under the new rules | passed |
| L7 | Snapshot expiry of everything untagged | Gate A maintenance, `test_a_release_..._expiry` | releases and the incremental base survive | passed: 2 releases reproduce |
| L8 | Orphan files left by a crashed writer | `test_maintenance_finds_and_removes_orphans_without_losing_rows` | found, removed, no row lost | passed (after D6) |
| L9 | Release crash before publishing | `test_a_release_interrupted_before_publishing_is_redone` | partial rows removed, release redone | passed |
| L10 | Write outside a Silver build | `test_reconciliation_passes_and_catches_a_write_outside_a_build` | reconciliation fails exactly that check | passed |
| L11 | Tampered final Gold day | Gate A Trino stage `frozen` | frozen on a normal run, restored on reopen | passed |
| L12 | Loss of the whole lakehouse (catalog and objects) | `scripts/lakehouse_dr.sh restore-check` | restore equals the backup; releases reproduce | passed: 15/15 tables, 24 s |
| L13 | Fresh catalog never touched by Trino | Airflow maintenance DAG, first run | jobs work on a V0 JDBC catalog | failed first (D10), passed after the fix |
| L14 | Lake with no Silver build yet | Airflow maintenance DAG | reconcile reports nothing to check | failed first (D11), passed after the fix |
| L15 | Airflow scheduler killed during Silver | Gate B | run completes after restart; reconciliation passes | passed: Silver ran twice, the run succeeded |
| L16 | Silver's job container killed | Gate B | task retried; reconciliation passes | passed: Silver ran twice, the run succeeded; the scenario now fails unless Silver is retried (D20) |
| L17 | Object store down at Bronze | Gate B | Bronze retried until Garage is back | passed: Bronze failed once while Garage was down, then succeeded |
| L18 | Registry down when Bronze looks up a new schema id | Gate B | Bronze retried; no record misclassified | passed: attempt 1 exited 75 with `RegistryUnavailable` for schema id 3, attempt 2 archived it, Silver and reconciliation passed (D19 made this provable) |
| L19 | Completed run repeated | Gate B | every step idempotent | passed: a run with no new data succeeded |
| L20 | Backfill overlapping the pipeline | Gate B | writer pool serialises them; both succeed | passed: both succeeded |

## Operational path (Milestone 2), rerun on the final code (2026-11-14)

An isolated Compose project (`fleetforge-final`) on fresh volumes; the preserved
`fleetforge` volumes were not touched. 19 non-disruptive and 7 disruptive stack tests
passed, plus the new restart test.

| # | Fault | Test | Result |
|---|---|---|---|
| M1 | Debezium restarted | `test_debezium_restart_resumes_from_committed_offset` | passed |
| M2 | PostgreSQL restarted under CDC | `test_postgres_restart_cdc_recovers` | passed |
| M3 | Debezium killed during mission updates | `test_debezium_killed_during_mission_updates_is_detected_and_recovers` | passed |
| M4 | Kafka outage longer than the producer timeout | `test_outage_longer_than_message_timeout_is_reported_not_hidden` | passed |
| M5 | Database restart while Kafka buffers | `test_database_restart_while_kafka_buffers` | passed |
| M6 | Registry outage | `test_registry_outage_pauses_projection_and_never_quarantines` | passed |
| M7 | Registry restart | `test_registry_restart_never_turns_valid_events_into_malformed` | passed |
| M8 | API container restart | `test_api_serves_again_after_a_container_restart` | failed first (D12), passed after the fix |
| M9 | Unanswered registry request | Milestone 2 CI stack job | hung for 600 s (D13); bounded by a 10 s timeout now |

Junit: `docs/reviews/evidence/final-junit-stack-disruptive.xml`.
