# AWS infrastructure (unverified deployment)

Terraform for the production shape of the lakehouse. It is formatted, validated,
planned and scanned in CI (`.github/workflows/platform.yml`) with placeholder
credentials and `offline_plan = true`. **It has never been applied**: applying it
creates billable resources and needs explicit approval and a real account.

| File | What it creates |
|---|---|
| `storage.tf` | KMS key (rotation on), lakehouse bucket (SSE-KMS, versioning, TLS-only, lifecycle, access logs) and the log bucket |
| `network.tf` | VPC with private subnets only, S3 gateway endpoint, interface endpoints, flow logs, security groups |
| `database.tf` | RDS PostgreSQL 17 for the Iceberg catalog and Airflow metadata (encrypted, forced TLS, backups, deletion protection) |
| `compute.tf` | ECR repositories, ECS cluster, Spark job task definition, task and execution roles |
| `kafka.tf` | Optional MSK Serverless with IAM authentication (`enable_msk`) |

```sh
terraform init
terraform plan -var account_id=<id>    # review; apply only with approval
```
