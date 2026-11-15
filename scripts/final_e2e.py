"""End-to-end check against an independent oracle: one fleet, two pipelines, one truth.

  m2    (host): publish a faulty fleet through Milestone 2 (API registrations, Kafka),
        wait for the projector, then compare PostgreSQL with the simulator's oracle.
  lake  (Spark container, Kafka overlay): Bronze from the same topics, Silver, then
        compare Silver per robot and per delivery with the same oracle.
Evidence: build/lakehouse-evidence/final-e2e.json.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

EVIDENCE = Path(os.environ.get("E2E_EVIDENCE", "build/lakehouse-evidence/final-e2e.json"))
SEED, ROBOTS, TICKS = 4242, 40, 300
# Milestone 2 calls a late event, and any redelivery of it, STALE; Silver accepts the
# first copy and calls later copies DUPLICATE. Both are compared as "kept".
KEPT = {"APPLIED", "STALE", "DUPLICATE", "ACCEPTED"}


def normal(outcome: str, reason: str | None) -> str:
    if outcome in KEPT:
        return "KEPT"
    if outcome == "CONFLICT":
        return "REJECTED:SEQUENCE_CONFLICT"
    return f"{outcome}:{reason}"


def fleet():
    from fleetforge.simulator.fleet import FaultConfig, SimConfig, generate

    faults = FaultConfig(
        duplicate_rate=0.05,
        reorder_rate=0.05,
        invalid_battery_rate=0.02,
        missing_field_rate=0.02,
        reboot_rate=0.01,
    )
    return generate(SimConfig(seed=SEED, robots=ROBOTS, ticks=TICKS, faults=faults))


def oracle(items) -> dict:
    from fleetforge.simulator.oracle import expected

    exp = expected(items, transport="kafka")
    deliveries = Counter()
    for (key, outcome, reason), n in exp.event_outcomes.items():
        deliveries[f"{key}|{normal(outcome, reason)}"] += n
    latest = {str(r): p["event_id"] for r, p in exp.latest.items()}
    return {"latest": latest, "deliveries": dict(deliveries)}


def m2_phase() -> None:
    from sqlalchemy import create_engine

    from fleetforge.simulator.fleet import Telemetry
    from fleetforge.simulator.kafka_publisher import HttpRegistrar, publish

    sys.path.insert(0, "tests")
    from stack.conftest import API, BOOTSTRAP, REGISTRY, _env_file, ledger_caught_up
    from support import assert_state_matches_oracle, db_state

    env = {**_env_file(), **os.environ}
    db_url = (
        f"postgresql+psycopg://{env.get('POSTGRES_USER', 'fleetforge')}:"
        f"{env['POSTGRES_PASSWORD']}@localhost:5432/fleetforge"
    )
    items = fleet()
    start = time.monotonic()
    report = publish(
        items,
        bootstrap=BOOTSTRAP,
        registry_url=REGISTRY,
        topic="telemetry.raw.v1",
        registrar=HttpRegistrar(API),
    )
    assert not report.failed and report.delivered == report.produced
    db = create_engine(db_url)
    deadline = time.monotonic() + 300
    while not ledger_caught_up(db, "telemetry-projector", "telemetry.raw.v1"):
        assert time.monotonic() < deadline, "projector did not catch up"
        time.sleep(2)
    caught_up_s = time.monotonic() - start
    from fleetforge.simulator.oracle import expected

    robots = {i.robot_id for i in items if isinstance(i, Telemetry)}
    state = db_state(db, robots)
    assert_state_matches_oracle(state, expected(items, transport="kafka"))
    truth = oracle(items)
    EVIDENCE.write_text(
        json.dumps(
            {
                "seed": SEED,
                "robots": ROBOTS,
                "ticks": TICKS,
                "produced": report.produced,
                "m2": {
                    "matches_oracle": True,
                    "robots": len(state),
                    "caught_up_s": round(caught_up_s, 1),
                    "latest": {str(r): str(v["event_id"]) for r, v in state.items()},
                },
                "oracle": truth,
            },
            indent=1,
        )
    )
    print(
        json.dumps(
            {
                "produced": report.produced,
                "m2_matches_oracle": True,
                "caught_up_s": round(caught_up_s, 1),
            }
        )
    )


def lake_phase() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from fleetforge.lakehouse.jobs import bronze_ingest as bj
    from fleetforge.lakehouse.jobs import silver_build as sj
    from fleetforge.lakehouse.jobs.common import fq, session
    from fleetforge.lakehouse.tables import (
        SILVER_TELEMETRY_EVENTS,
        SILVER_TELEMETRY_RECORD_OUTCOMES,
    )

    evidence = json.loads(EVIDENCE.read_text())
    truth = evidence["oracle"] = oracle(fleet())  # deterministic: same seed as the m2 phase
    bronze = bj.run(bj_args())
    spark = session("final-e2e")
    built = sj.build_with_retry(spark, full=True)
    events = spark.table(fq(SILVER_TELEMETRY_EVENTS)).where("is_current")
    newest = Window.partitionBy("robot_id").orderBy(
        F.desc("boot_number"), F.desc("sequence_number")
    )
    latest = {
        r["robot_id"]: r["event_id"]
        for r in events.withColumn("n", F.row_number().over(newest)).where("n = 1").collect()
    }
    from fleetforge.domain.telemetry import dedup_key, identity_hints, payload_for_audit
    from fleetforge.lakehouse.tables import BRONZE_KAFKA_RECORDS

    bodies = spark.table(fq(BRONZE_KAFKA_RECORDS)).select(
        "kafka_topic_id", "kafka_partition", "kafka_offset", "record_value"
    )
    rows = (
        spark.table(fq(SILVER_TELEMETRY_RECORD_OUTCOMES))
        .join(bodies, ["kafka_topic_id", "kafka_partition", "kafka_offset"])
        .collect()
    )
    deliveries, accepted = Counter(), Counter()
    for o in rows:
        key = o["event_id"]
        if key is None:  # unreadable: keyed by its bytes, as Milestone 2 does
            body = bytes(o["record_value"] or b"")
            key = dedup_key(identity_hints(payload_for_audit(body))[0], body)
        deliveries[f"{key}|{normal(o['outcome'], o['reason_code'])}"] += 1
        accepted[o["event_id"]] += o["outcome"] == "ACCEPTED"
    want = Counter(truth["deliveries"])
    latest_diff = {
        r: (truth["latest"].get(r), latest.get(r))
        for r in truth["latest"].keys() | latest.keys()
        if truth["latest"].get(r) != latest.get(r)
    }
    deliveries_diff = {
        k: (want.get(k, 0), deliveries.get(k, 0))
        for k in want.keys() | deliveries.keys()
        if want.get(k, 0) != deliveries.get(k, 0)
    }
    twice = [e for e, n in accepted.items() if n > 1]
    evidence["lake"] = {
        "events_accepted_more_than_once": len(twice),
        "bronze": {k: bronze[k] for k in ("new_rows", "ranges")},
        "silver_build": built["build_id"],
        "telemetry_rows": built["telemetry_rows"],
        "latest_matches_oracle": not latest_diff,
        "latest_diff": latest_diff,
        "deliveries_match_oracle": not deliveries_diff,
        "deliveries_diff": dict(list(deliveries_diff.items())[:20]),
        "deliveries_diff_count": len(deliveries_diff),
    }
    EVIDENCE.write_text(json.dumps(evidence, indent=1, default=str))
    print(json.dumps(evidence["lake"], default=str))
    assert not latest_diff and not deliveries_diff and not twice


def bj_args():
    import argparse

    return argparse.Namespace(
        topic=None,
        max_records_per_partition=None,
        accept_gaps=False,
        accept_new_incarnation=None,
        replay=False,
    )


if __name__ == "__main__":
    {"m2": m2_phase, "lake": lake_phase}[sys.argv[1]]()
