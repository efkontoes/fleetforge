# ADR 0010 — Least-privilege roles, generated secrets, resource-aware profiles

Status: accepted (Milestone 2)

## Decision
* Five PostgreSQL roles: app, projector, mission consumer, CDC (only one with
  REPLICATION), read-only (`pg_monitor`). Each has a connection limit. Created
  NOLOGIN by migration; login passwords are set from `.env` by
  `fleetforge.ops.provision`. One extra column privilege (migration 0006) was
  found by running the API as its role.
* `.env` is generated with random secrets (`make env`, mode 600, git-ignored).
  Compose refuses to start without them (`${VAR:?}`).
* Host ports bind to 127.0.0.1. Kafka, Connect REST and the registry are
  unauthenticated PLAINTEXT, **acceptable only on a single-user machine**.
  Production: TLS + SASL/SCRAM or mTLS, ACLs per principal, Connect REST behind
  auth, PostgreSQL `hostssl` with scram-sha-256.
* Compose profiles (`core`, `processing`, `cdc`, `observability`, `full`) with JVM
  heaps and memory limits. Measured: the full stack leaves ~0.7 GiB free in a
  3.7 GiB WSL VM.
* WSL memory is **not** raised. The host has 7.8 GiB with ~0.8 GiB free; giving
  WSL more would starve Windows.
* The GitHub token was moved from plaintext `~/.config/gh/hosts.yml` to Windows
  Credential Manager via Git Credential Manager. `gh` reads it through a wrapper.

## Not done (production gaps)
TLS everywhere, Kafka ACLs, secret manager/rotation, the owner role being
superuser locally, and point-in-time recovery (only logical backups are tested).
