"""Monthly AWS cost of the lakehouse from measured sizes and timings and list prices.

Inputs: Gate A evidence (bytes per row per table, job durations) and the price file
written by scripts/aws_prices.py. Everything not measured is an explicit assumption
below, so a reader can change it and rerun.
python3 scripts/cost_model.py > docs/reviews/evidence/cost-model.md
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

GATE_A = Path("docs/reviews/evidence/gate-a.json")
SCALE = Path("docs/reviews/evidence/scale-silver.json")
PRICES = Path("docs/reviews/evidence/aws-prices-eu-west-1.json")
HOURS = 730  # AWS's month

ASSUMPTIONS = {
    "pipeline_runs_per_month": 30 * 24 * 6,  # every 10 minutes
    "retention_months": 12,  # Bronze and Silver kept for a year
    "task_vcpu": 1,
    "task_gb": 3,
    "s3_puts_per_run": 60,  # data, manifests, metadata across the run's commits
    "s3_gets_per_run": 600,
    "interface_endpoints": 5,
    "availability_zones": 2,
    "log_gb_per_month": 2,
    "ecr_gb": 6,
    "trino_hours_per_day": 8,  # on demand for analysts, 2 vCPU / 8 GB
    "kafka_partitions": 13,
    "fleets": {"50 robots at 1 Hz": 50, "500 robots at 1 Hz": 500},
}


def measured(gate: dict, scale: dict) -> dict:
    storage = {r["table_name"]: r for r in gate["maintenance"]}
    lake_bytes = sum(r["data_bytes"] for r in storage.values())
    full = scale["full_1m"]
    return {
        "bytes_per_event": lake_bytes / gate["full_digest"]["telemetry_rows"],  # Gate A
        "bronze_rows_per_s": 1_036_574 / scale["bronze_full_s"],  # 1 M-record ingest
        "silver_full_rows_per_s": full["telemetry_rows"] / max(scale["silver_full_s_wall"]),
        "fixed_run_s": 57 + scale["silver_delta_s_wall_after"],  # Bronze + Silver, small delta
        "history_rows_at_fixed": full["telemetry_rows"],
    }


def monthly(price: dict, m: dict, robots: int) -> dict[str, float]:
    a = ASSUMPTIONS
    p = {k: v["usd"] for k, v in price["prices"].items()}
    events = robots * 3600 * 24 * 30
    stored_gb = events * a["retention_months"] * m["bytes_per_event"] / 1e9
    rows_per_run = events / a["pipeline_runs_per_month"]
    # Fixed cost of a run (measured at 1 M rows of history) plus the measured per-row rates.
    per_row = 1 / m["bronze_rows_per_s"] + 1 / m["silver_full_rows_per_s"]
    run_s = m["fixed_run_s"] + rows_per_run * per_row
    task_hour = a["task_vcpu"] * p["fargate_vcpu_hour"] + a["task_gb"] * p["fargate_gb_hour"]
    trino_hour = 2 * p["fargate_vcpu_hour"] + 8 * p["fargate_gb_hour"]
    return {
        "S3 storage (12 months of data)": stored_gb * p["s3_standard_gb_month"],
        "S3 requests": a["pipeline_runs_per_month"]
        * (a["s3_puts_per_run"] * p["s3_put_request"] + a["s3_gets_per_run"] * p["s3_get_request"]),
        "Fargate job tasks": a["pipeline_runs_per_month"] * run_s / 3600 * task_hour,
        "Trino on demand": a["trino_hours_per_day"] * 30 * trino_hour,
        "RDS db.t4g.small + 20 GB gp3": HOURS * p["rds_db_t4g_small_hour"]
        + 20 * p["rds_gp3_gb_month"],
        "VPC interface endpoints": a["interface_endpoints"] * a["availability_zones"] * HOURS
        * p["vpc_endpoint_hour"],
        "KMS, secret, ECR, logs": p["kms_key_month"] + p["secret_month"]
        + a["ecr_gb"] * p["ecr_gb_month"] + a["log_gb_per_month"]
        * (p["logs_ingest_gb"] + p["logs_storage_gb_month"]),
        "MSK Serverless (optional)": HOURS * (p["msk_serverless_cluster_hour"]
        + a["kafka_partitions"] * p["msk_serverless_partition_hour"])
        + events * 300 / 1e9 * (p["msk_serverless_in_gb"] + p["msk_serverless_out_gb"]),
        "MWAA micro (or Airflow on ECS)": HOURS * p["mwaa_micro_environment_hour"],
        "_run_seconds": run_s,
        "_stored_gb": stored_gb,
    }  # fmt: skip


def main() -> None:
    gate, price = json.loads(GATE_A.read_text()), json.loads(PRICES.read_text())
    m = measured(gate, json.loads(SCALE.read_text()))
    published = min(v["published"] for v in price["prices"].values())[:10]
    out = ["# Lakehouse cost model (eu-west-1, on-demand list prices)", "",
           f"Prices from the AWS Price List, published {published} or later ({PRICES}). "
           f"Measured inputs from Gate A ({GATE_A}) and the 1 M-row run ({SCALE}).",
           "", "## Measured", ""]  # fmt: skip
    out += [f"* {k}: {v:,.2f}" if isinstance(v, float) else f"* {k}: {v:,}" for k, v in m.items()]
    out += ["", "## Assumptions (not measured)", ""]
    out += [f"* {k}: {v}" for k, v in ASSUMPTIONS.items() if k != "fleets"]
    for label, robots in ASSUMPTIONS["fleets"].items():
        cost = monthly(price, m, robots)
        out += ["", f"## {label}", "", "| Item | USD / month |", "|---|---:|"]
        out += [f"| {k} | {v:,.2f} |" for k, v in cost.items() if not k.startswith("_")]
        total = sum(v for k, v in cost.items() if not k.startswith("_"))
        out += [f"| **Total** | **{total:,.2f}** |", "",
                f"Pipeline run: {cost['_run_seconds']:.0f} s every 10 minutes; "
                f"data kept after 12 months: {cost['_stored_gb']:,.1f} GB."]  # fmt: skip
    out += ["", "Caveats: bytes per event come from a small corpus, where Parquet and "
            "metadata overhead weigh more than at scale. The fixed run cost was measured at "
            "1 M rows of history; the closure scans a key projection of all prepared rows, so "
            "it grows with history, and a year of 500 robots needs a per-robot boot index "
            "(ADR 0017) before these run times hold."]  # fmt: skip
    print("\n".join(out))


if __name__ == "__main__":
    sys.exit(main())
