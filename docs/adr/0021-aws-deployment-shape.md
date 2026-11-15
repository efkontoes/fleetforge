# ADR 0021: The AWS shape of the platform, as code that is planned but not applied

Status: accepted as a design. The Terraform is formatted, validated, planned and scanned
in CI. It has not been applied, and the deployment is unverified.

## Context
Everything so far runs locally on Docker. A reviewer needs to see how the same jobs run
in a cloud account, what it would cost, and that the configuration is safe. Nothing may
be provisioned or billed without explicit approval.

## Decision
* **Keep the job model.** Each job is the same container image, started as an ECS
  Fargate task (1 vCPU, 3 GiB). This is the AWS twin of a `DockerOperator` task. Spark
  stays in local mode, which is sized for the measured data.
* **Storage and catalog.**
  * S3 with SSE-KMS, versioning, a TLS-only policy and access logs.
  * The Iceberg JDBC catalog on RDS PostgreSQL 17, which also holds Airflow metadata.
  * Trino and Spark keep the same catalog configuration as locally.
* **Network.**
  * Private subnets with no NAT gateway.
  * S3 through a gateway endpoint (free); ECR, Logs, Secrets Manager and STS through
    interface endpoints.
  * Security groups admit the database and Kafka only from job tasks.
* **Kafka is optional** (`enable_msk`): MSK Serverless with IAM authentication. A
  Serverless cluster bills per hour even when idle.
* **CI plans without credentials.** The provider skips every account lookup when
  `offline_plan = true`, and the configuration contains no data source that calls AWS.
  CI runs `fmt`, `validate`, `plan` and `trivy config` on every pull request.

## Alternatives
| Option | Why not |
|---|---|
| EMR Serverless for Spark | The Spark and Iceberg versions come with the EMR release instead of our pinned, tested ones, and local mode already fits the measured data |
| MWAA for Airflow | The smallest (micro) environment lists at USD 0.29 an hour in eu-west-1, about USD 212 for a 730-hour month, before any work is done (`docs/reviews/evidence/aws-prices-eu-west-1.json`) |
| Athena instead of Trino | It needs the Glue catalog; ours is JDBC, and changing that also changes Spark |
| NAT gateway | Hourly charge and data processing for traffic that endpoints already carry |

## Consequences
* Orchestration in AWS is left as a choice, MWAA or Airflow on ECS, because Airflow is
  the largest fixed cost; the cost model in the final report prices both.
* A real deployment still needs to be done: image pushes, a remote state backend, DNS,
  and the first `apply` with a budget alarm in place.
