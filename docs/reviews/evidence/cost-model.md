# Lakehouse cost model (eu-west-1, on-demand list prices)

Prices from the AWS Price List, published 2026-11-03 or later (docs/reviews/evidence/aws-prices-eu-west-1.json). Measured inputs from Gate A (docs/reviews/evidence/gate-a.json) and the 1 M-row run (docs/reviews/evidence/scale-silver.json).

## Measured

* bytes_per_event: 303.53
* bronze_rows_per_s: 8,098.23
* silver_full_rows_per_s: 2,150.24
* fixed_run_s: 181
* history_rows_at_fixed: 1,057,918

## Assumptions (not measured)

* pipeline_runs_per_month: 4320
* retention_months: 12
* task_vcpu: 1
* task_gb: 3
* s3_puts_per_run: 60
* s3_gets_per_run: 600
* interface_endpoints: 5
* availability_zones: 2
* log_gb_per_month: 2
* ecr_gb: 6
* trino_hours_per_day: 8
* kafka_partitions: 13

## 50 robots at 1 Hz

| Item | USD / month |
|---|---:|
| S3 storage (12 months of data) | 10.86 |
| S3 requests | 2.33 |
| Fargate job tasks | 12.83 |
| Trino on demand | 27.96 |
| RDS db.t4g.small + 20 GB gp3 | 28.09 |
| VPC interface endpoints | 80.30 |
| KMS, secret, ECR, logs | 3.20 |
| MSK Serverless (optional) | 652.70 |
| MWAA micro (or Airflow on ECS) | 211.70 |
| **Total** | **1,029.98** |

Pipeline run: 199 s every 10 minutes; data kept after 12 months: 472.0 GB.

## 500 robots at 1 Hz

| Item | USD / month |
|---|---:|
| S3 storage (12 months of data) | 108.57 |
| S3 requests | 2.33 |
| Fargate job tasks | 23.09 |
| Trino on demand | 27.96 |
| RDS db.t4g.small + 20 GB gp3 | 28.09 |
| VPC interface endpoints | 80.30 |
| KMS, secret, ECR, logs | 3.20 |
| MSK Serverless (optional) | 713.06 |
| MWAA micro (or Airflow on ECS) | 211.70 |
| **Total** | **1,198.31** |

Pipeline run: 358 s every 10 minutes; data kept after 12 months: 4,720.5 GB.

Caveats: bytes per event come from a small corpus, where Parquet and metadata overhead weigh more than at scale. The fixed run cost was measured at 1 M rows of history; the closure scans a key projection of all prepared rows, so it grows with history, and a year of 500 robots needs a per-robot boot index (ADR 0017) before these run times hold.
