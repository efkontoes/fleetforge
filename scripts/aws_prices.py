"""Fetch the AWS list prices the cost model uses from the public AWS Price List bulk files.

No credentials and no account: these are the published on-demand prices. Output records
each price with its SKU, description and the price list's publication date.
python3 scripts/aws_prices.py [--region eu-west-1] > docs/reviews/evidence/aws-prices.json
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import urllib.request

URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/{region}/index.csv"

# name -> (service, usage type, extra filters); the usage-type prefix is "EU-" for eu-west-1.
WANTED = {
    "s3_standard_gb_month": ("AmazonS3", "{p}-TimedStorage-ByteHrs", {"StartingRange": "0"}),
    "s3_put_request": ("AmazonS3", "{p}-Requests-Tier1", {}),
    "s3_get_request": ("AmazonS3", "{p}-Requests-Tier2", {}),
    "fargate_vcpu_hour": ("AmazonECS", "{p}-Fargate-vCPU-Hours:perCPU", {}),
    "fargate_gb_hour": ("AmazonECS", "{p}-Fargate-GB-Hours", {}),
    "rds_db_t4g_small_hour": (
        "AmazonRDS",
        "{p}-InstanceUsage:db.t4g.small",
        {"Database Engine": "PostgreSQL"},
    ),
    "rds_gp3_gb_month": ("AmazonRDS", "{p}-RDS:GP3-Storage", {}),
    "msk_serverless_cluster_hour": ("AmazonMSK", "{p}-KafkaServerless-ClusterHours", {}),
    "msk_serverless_partition_hour": ("AmazonMSK", "{p}-KafkaServerless-PartitionHours", {}),
    "msk_serverless_in_gb": ("AmazonMSK", "{p}-KafkaServerless-In-Bytes", {}),
    "msk_serverless_out_gb": ("AmazonMSK", "{p}-KafkaServerless-Out-Bytes", {}),
    "msk_serverless_storage_gb_month": ("AmazonMSK", "{p}-KafkaServerless-StorageHours", {}),
    "vpc_endpoint_hour": ("AmazonVPC", "{p}-VpcEndpoint-Hours", {}),
    "vpc_endpoint_gb": ("AmazonVPC", "{p}-VpcEndpoint-Bytes", {"StartingRange": "0"}),
    "logs_ingest_gb": ("AmazonCloudWatch", "{p}-DataProcessing-Bytes", {}),
    "logs_storage_gb_month": ("AmazonCloudWatch", "{p}-TimedStorage-ByteHrs", {}),
    "ecr_gb_month": ("AmazonECR", "{p}-TimedStorage-ByteHrs", {}),
    "kms_key_month": ("awskms", "{r}-KMS-Keys", {}),
    "secret_month": ("AWSSecretsManager", "{p}-AWSSecretsManager-Secrets", {}),
    "mwaa_micro_environment_hour": ("AmazonMWAA", "{p}-Airflow-MicroEnvironment", {}),
    "mwaa_small_environment_hour": ("AmazonMWAA", "{p}-Airflow-SmallEnvironment", {}),
}
PREFIX = {"eu-west-1": "EU", "us-east-1": "USE1", "eu-central-1": "EUC1"}


def price_list(service: str, region: str) -> tuple[str, list[dict[str, str]]]:
    with urllib.request.urlopen(URL.format(service=service, region=region), timeout=60) as r:
        text = io.TextIOWrapper(r, encoding="utf-8", newline="")
        meta = dict(next(csv.reader([text.readline()])) for _ in range(5))
        return meta["Publication Date"], list(csv.DictReader(text))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="eu-west-1")
    region = ap.parse_args().region
    lists: dict[str, tuple[str, list[dict[str, str]]]] = {}
    out = {"region": region, "currency": "USD", "source": URL.split("{")[0], "prices": {}}
    for name, (service, usage, filters) in WANTED.items():
        if service not in lists:
            lists[service] = price_list(service, region)
        published, rows = lists[service]
        usage = usage.format(p=PREFIX[region], r=region)
        match = [
            r
            for r in rows
            if r["TermType"] == "OnDemand"
            and r.get("usageType") == usage
            and all(r.get(k) == v for k, v in filters.items())
        ]
        prices = {r["PricePerUnit"] for r in match}
        if len(prices) != 1:
            raise SystemExit(f"{name}: expected one price for {usage}, found {sorted(prices)}")
        r = match[0]
        out["prices"][name] = {
            "usd": float(r["PricePerUnit"]),
            "unit": r["Unit"],
            "sku": r["SKU"],
            "description": r["PriceDescription"],
            "published": published,
        }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
