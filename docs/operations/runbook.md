# Operations runbook (local development)

## Development environment and memory

Measured on 2026-11-12:

| | |
|---|---|
| Windows | 7.8 GiB RAM total, ~0.8 GiB free while WSL and Docker run |
| CPU | i5-10210U, 4 cores / 8 threads |
| WSL2 | default limit = 50 % of RAM → 3.7 GiB; 1 GiB swap; no `.wslconfig` |
| Disk | 950 GiB free; synced 8 KB write ≈ 2 ms; PostgreSQL durable commit ≈ 4.6 ms |
| `full` profile | ~3.0 GiB used, ~0.7 GiB available inside WSL |

**Decision: do not raise the WSL memory limit.** Windows already runs with
~0.8 GiB free; giving WSL more would make Windows page. Instead, every JVM has an
explicit heap, every container a memory limit, and profiles keep the default
footprint small.

If the host gets more RAM (16 GiB), a reasonable allocation is 8 GiB for WSL
(half), leaving 8 GiB for Windows. Configure it on the **Windows** side:

```ini
# C:\Users\<you>\.wslconfig
[wsl2]
memory=8GB
swap=4GB
processors=6
```

Then, from PowerShell: `wsl --shutdown`, wait ~10 s, reopen the WSL terminal, and
check with `free -h`. `wsl --shutdown` stops every WSL distribution and Docker,
so do it with nothing running.

## Profiles

| Command | Starts | Approx. memory |
|---|---|---|
| `make up` | PostgreSQL | 0.1 GiB |
| `make up-core` | + Kafka, registry, migrations, API | 1.3 GiB |
| `make up-processing` | + telemetry projector | 1.4 GiB |
| `make up-full` | + Debezium, mission consumer, Prometheus, Grafana, exporters | 3.0 GiB |

## Credentials

* Database, registry and Grafana secrets: `make env` writes `.env` with random
  values (mode 600, git-ignored). Compose refuses to start without them. Rotate
  them by editing `.env` and re-running `docker compose --profile core up migrate`
  (provisioning sets the passwords again).
* GitHub: the token is stored in **Windows Credential Manager** through Git
  Credential Manager (`git config --global credential.helper` points at the
  Windows GCM binary). `gh` reads it through `/usr/local/bin/gh`, a wrapper that
  sets `GH_TOKEN` from `git credential fill`. `~/.config/gh/hosts.yml` contains no
  token. To log in again from scratch:
  `gh auth login --web`, then `gh auth token` piped into
  `git-credential-manager.exe store`, then `/usr/bin/gh auth logout`.

## Backups

`make backup-check` runs `pg_dump -Fc`, restores into a scratch database, and
compares row counts for every `fleet`/`downstream` table (verified 2026-11-12:
11 tables identical). Dumps go to `./backups/` (git-ignored). This is a *logical*
backup: no point-in-time recovery. After restoring an older dump:
* consumers re-read from Kafka where their ledger rows are older (safe:
  idempotent);
* Debezium slots do not exist in a restored instance; recreate the connectors
  (snapshot `initial` re-publishes existing outbox rows, consumers deduplicate by
  version).

## Replication slots

* Inspect: `select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) from pg_replication_slots;`
* Alerts: `ReplicationSlotRetainingWal` (> 512 MiB for 10 min), `ReplicationSlotInactive`.
* Hard cap: `max_slot_wal_keep_size=2GB`. Beyond it the slot is invalidated:
  delete and re-register the connector (it re-snapshots).
* Removing CDC permanently: delete the connectors **and** the slots:
  `select pg_drop_replication_slot('fleetforge_outbox');`. An orphaned slot
  retains WAL until the cap.

## Retention

* Outbox: `python -m fleetforge.ops.retention --days 7` deletes in chunks, also
  trims `fleet.cdc_heartbeat`, and refuses while the outbox slot is inactive or
  more than 64 MiB behind.
* Kafka: telemetry 7 days, business events 30 days. That is also the replay
  horizon until the M3 bronze layer exists.
* Consumer offsets: Kafka keeps committed offsets for 7 days after a group goes
  empty (`offsets.retention.minutes` default). The PostgreSQL ledger keeps one row
  per group/partition indefinitely; delete a decommissioned group's rows explicitly.

## Local security assumptions (not production)

| Area | Local | Production would need |
|---|---|---|
| Network | ports bound to 127.0.0.1 | private subnets, security groups |
| Kafka | PLAINTEXT, no auth, RF=1 | TLS + SASL/SCRAM or mTLS, ACLs per principal, RF=3, `min.insync.replicas=2` |
| Kafka Connect REST | unauthenticated on 127.0.0.1 | auth proxy, network isolation |
| Schema registry | unauthenticated | auth, read-only clients, CI-only writes |
| PostgreSQL | password auth, no TLS, owner is superuser | `hostssl` + scram-sha-256, non-superuser owner, `rds_replication` |
| Secrets | `.env` file | secret manager with rotation |
| Grafana | admin password from `.env` | SSO |
