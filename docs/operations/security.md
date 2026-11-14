# Security: local configuration versus production

The local stacks are built for one developer on one machine. This page lists every place
where local differs from what a production deployment needs. The Terraform in
`infrastructure/terraform` implements the production column for AWS; it has been
validated and planned, never applied (see ADR 0021).

| Concern | Local (this repository) | Production |
|---|---|---|
| Secrets | random per machine, written by `scripts/make-lakehouse-env.sh` to git-ignored files with mode 600; never printed | AWS Secrets Manager; the RDS master password is managed by RDS and rotated; tasks read secrets at start through the execution role |
| Object storage | Garage on localhost, one access key with full bucket rights | S3 with SSE-KMS (key rotation on), public access blocked, TLS-only bucket policy, versioning, access logs; the jobs' role may only touch `warehouse/` and `exports/` |
| Catalog database | PostgreSQL 17 in a container, superuser `iceberg` | RDS PostgreSQL 17, encrypted with KMS, `rds.force_ssl=1`, IAM authentication available, 7-day backups, deletion protection, private subnets only |
| Network | services bound to 127.0.0.1; Docker networks | private subnets with no internet route; S3 through a gateway endpoint, other AWS APIs through interface endpoints; security groups admit only the job tasks; VPC flow logs for rejected traffic |
| Kafka | plaintext inside the Docker network | MSK Serverless with IAM authentication (port 9098), reachable only from the job security group |
| Orchestration | Airflow with the simple auth manager and one admin user; Docker reached through a socket proxy limited to containers, images and networks | managed Airflow, or Airflow on ECS behind SSO; tasks start as ECS `RunTask` with a task role instead of a Docker socket |
| Images | built locally, tag `dev` | ECR with immutable tags, scan on push, KMS encryption; deploy by digest |
| Logs | container stdout | CloudWatch Logs with KMS and 30-day retention |
| Data in logs | Airflow masks variables whose names contain `password`, `secret` or `access_key`; jobs never log payloads | same, plus log-group access limited to operators |

## Things that are deliberately weaker locally
* The catalog role is a superuser. Production would separate a schema-owning migration
  role from the job role, as Milestone 2 does for its own database.
* Airflow's socket proxy still allows starting containers, which on one host is close
  to root. Acceptable on a laptop, not on a shared host.
* Trino has no authentication locally. In production it sits behind TLS and LDAP, OAuth
  or IAM-backed authentication, or Athena replaces it (that would need a Glue catalog).

## Checks that run in CI
* `pip-audit` on the lakehouse lock file.
* `trivy config` on the Terraform: any finding fails the build. Accepted
  exceptions are inline `#trivy:ignore` comments, each with its reason.
* Secrets are generated per run, so no real secret is ever committed.
