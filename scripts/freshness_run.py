"""Measure the freshness SLO on a real cadence: records arrive all the time, the pipeline
runs every 10 minutes, and the freshness job reports how late each record reached Silver.

python3 scripts/freshness_run.py --cycles 6 --cycle-s 600   (Milestone 2 kafka + registry up)
Evidence: build/lakehouse-evidence/freshness-run.json.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

EVIDENCE = Path("build/lakehouse-evidence/freshness-run.json")


def compose_run(env: dict[str, str], module: str, *args: str) -> dict:
    flags = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    cmd = [
        "docker",
        "compose",
        "--env-file",
        ".env.lakehouse",
        "-f",
        "infrastructure/lakehouse/compose.yaml",
        "-f",
        "infrastructure/lakehouse/compose.kafka.yaml",
        "--profile",
        "jobs",
        "run",
        "--rm",
        *flags,
        "spark",
        "-m",
        module,
        *args,
    ]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from fleetforge.kafka.schemas import register, registry_client, subject_for
    from fleetforge.lakehouse import fixtures as fx
    from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC
    from fleetforge.lakehouse.silver import TELEMETRY_TOPIC

    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=6)
    ap.add_argument("--cycle-s", type=int, default=600)
    ap.add_argument("--chunks", type=int, default=20)
    args = ap.parse_args()

    bootstrap = os.environ.get("FLEETFORGE_KAFKA_BOOTSTRAP", "127.0.0.1:29092")
    registry = os.environ.get(
        "FLEETFORGE_SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7"
    )
    gid = uuid.uuid4().hex[:8]
    topics = {
        TELEMETRY_TOPIC: f"fresh.{gid}.telemetry",
        ROBOT_EVENTS_TOPIC: f"fresh.{gid}.robots",
        FIRMWARE_TOPIC: f"fresh.{gid}.firmware",
    }
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new = [NewTopic(t, 3 if src == TELEMETRY_TOPIC else 1, 1) for src, t in topics.items()]
    for f in admin.create_topics(new).values():
        f.result(30)
    client = registry_client(registry)
    ids = {rev: register(client, subject_for(topics[TELEMETRY_TOPIC]), rev) for rev in (1, 2)}
    remap = {fx.SCHEMA_V1: ids[1], fx.SCHEMA_V2: ids[2], fx.UNKNOWN_THEN_FOUND: ids[1]}
    corpus = fx.random_corpus(17, robots=30, ticks=600, runs=args.cycles)
    env = {
        "FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL": "jdbc:postgresql://catalog-db:5432/iceberg_catalog_test",
        "FLEETFORGE_LAKEHOUSE_WAREHOUSE": "s3://lakehouse/test-warehouse",
        "FLEETFORGE_LAKEHOUSE_RECORD_RUNS": "false",
        "FLEETFORGE_LAKEHOUSE_TELEMETRY_TOPIC": topics[TELEMETRY_TOPIC],
        "FLEETFORGE_LAKEHOUSE_ROBOT_EVENTS_TOPIC": topics[ROBOT_EVENTS_TOPIC],
        "FLEETFORGE_LAKEHOUSE_FIRMWARE_TOPIC": topics[FIRMWARE_TOPIC],
    }
    subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            ".env.lakehouse",
            "-f",
            "infrastructure/lakehouse/compose.yaml",
            "--profile",
            "jobs",
            "run",
            "--rm",
            "-e",
            f"FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL={env['FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL']}",
            "-e",
            f"FLEETFORGE_LAKEHOUSE_WAREHOUSE={env['FLEETFORGE_LAKEHOUSE_WAREHOUSE']}",
            "spark",
            "/workspace/scripts/gate_a.py",
            "drop-tables",
        ],
        capture_output=True,
        check=True,
    )  # a clean disposable catalog
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    runs = []
    for cycle in range(args.cycles):
        batch = corpus.batches[cycle]
        step = max(1, len(batch) // args.chunks)
        pause = args.cycle_s / args.chunks
        start = time.monotonic()
        for i in range(0, len(batch), step):  # spread the batch over the cycle, as traffic
            for r in batch[i : i + step]:
                value = r.value
                if value and len(value) >= 5 and value[0] == 0:
                    sid = int.from_bytes(value[1:5], "big")
                    value = b"\x00" + remap[sid].to_bytes(4, "big") + value[5:]
                target = topics[r.topic]
                part = r.partition if r.topic == TELEMETRY_TOPIC else 0
                producer.produce(
                    target, key=r.key, value=value, partition=part, headers=list(r.headers)
                )
            producer.flush(30)
            time.sleep(max(0.0, start + pause * (i // step + 1) - time.monotonic()))
        t0 = time.monotonic()
        bronze = compose_run(env, "fleetforge.lakehouse.jobs.bronze_ingest")
        silver = compose_run(env, "fleetforge.lakehouse.jobs.silver_build")
        runs.append(
            {
                "cycle": cycle,
                "published": len(batch),
                "bronze_rows": bronze["new_rows"],
                "silver_mode": silver.get("mode"),
                "pipeline_s": round(time.monotonic() - t0),
            }
        )
        print(json.dumps(runs[-1]), flush=True)
    report = compose_run(env, "fleetforge.lakehouse.jobs.freshness", "--hours", "6")
    EVIDENCE.write_text(
        json.dumps(
            {"cadence_s": args.cycle_s, "runs": runs, "freshness": report}, indent=1, default=str
        )
    )
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
