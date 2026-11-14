"""Gate A: the lakehouse end to end on real engines, with evidence.

Spark phase (Kafka overlay, test catalog):  python scripts/gate_a.py spark
Trino phase (after dbt build):              python scripts/gate_a.py trino --stage first
Each job runs as its own process, as the scheduler runs it. Evidence: /evidence/gate-a.json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

EVIDENCE = Path(os.environ.get("GATE_EVIDENCE", "/evidence/gate-a.json"))
REFERENCE = EVIDENCE.with_name("gate-a-gold-reference.json")
SEED, ROBOTS, TICKS, RUNS = 11, 40, 1100, 4
MISSING_SCHEMA_ID = 2**31 - 7  # never assigned: these records stay UNRESOLVED
TAMPER = 1_000_000


def load() -> dict[str, Any]:
    return json.loads(EVIDENCE.read_text()) if EVIDENCE.exists() else {}


def save(evidence: dict[str, Any]) -> None:
    EVIDENCE.write_text(json.dumps(evidence, indent=2, default=str, sort_keys=True))


def cgroup_memory() -> int:
    for path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            return int(Path(path).read_text())
        except OSError:
            continue
    return 0


def job(evidence: dict, label: str, module: str, *args: str, expect_kill: bool = False) -> dict:
    """Run one job process; record its wall time and the container's peak memory meanwhile."""
    peak, done = [cgroup_memory()], threading.Event()

    def sample() -> None:
        while not done.wait(0.5):
            peak[0] = max(peak[0], cgroup_memory())

    threading.Thread(target=sample, daemon=True).start()
    start = time.monotonic()
    target = ["-m", module] if module.startswith("fleetforge") else [module]
    cmd = [sys.executable, *target, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    done.set()
    seconds = round(time.monotonic() - start, 1)
    if expect_kill:
        assert proc.returncode == -signal.SIGKILL, (proc.returncode, proc.stderr[-2000:])
        report: dict = {"killed": True}
    else:
        assert proc.returncode == 0, f"{label} failed:\n{proc.stderr[-4000:]}"
        report = json.loads(proc.stdout.strip().splitlines()[-1])
    evidence.setdefault("steps", []).append(
        {"step": label, "seconds": seconds, "peak_container_mib": peak[0] // 2**20}
    )
    print(f"{label}: {seconds}s, peak {peak[0] // 2**20} MiB", flush=True)
    return report


# --------------------------------------------------------------------------- Spark phase


def publish(corpus, run: int, topics: dict[str, str], remap: dict[int, int]) -> dict[str, int]:
    from confluent_kafka import Producer

    from fleetforge.config import get_settings

    p = Producer({"bootstrap.servers": get_settings().kafka_bootstrap, "enable.idempotence": True,
                  "linger.ms": 20})  # fmt: skip
    counts: dict[str, int] = {}
    for r in corpus.batches[run]:
        value = r.value
        if value and len(value) >= 5 and value[0] == 0:
            sid = int.from_bytes(value[1:5], "big")
            value = b"\x00" + remap[sid].to_bytes(4, "big") + value[5:]
        target = topics[r.topic]
        partition = r.partition if target == topics["telemetry"] else 0
        p.produce(target, key=r.key, value=value, partition=partition, headers=list(r.headers))
        counts[target] = counts.get(target, 0) + 1
        if len(p) > 50_000:
            p.poll(0.1)
    assert p.flush(60) == 0
    return counts


def make_topics(gid: str) -> dict[str, str]:
    from confluent_kafka.admin import AdminClient, NewTopic

    from fleetforge.config import get_settings
    from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC
    from fleetforge.lakehouse.silver import TELEMETRY_TOPIC

    names = {TELEMETRY_TOPIC: f"gate.{gid}.telemetry", ROBOT_EVENTS_TOPIC: f"gate.{gid}.robots",
             FIRMWARE_TOPIC: f"gate.{gid}.firmware",
             "missions": f"gate.{gid}.missions"}  # fmt: skip
    admin = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap})
    new = [NewTopic(n, 3 if src == TELEMETRY_TOPIC else 1, 1) for src, n in names.items()]
    for f in admin.create_topics(new).values():
        f.result(30)
    names["telemetry"] = names[TELEMETRY_TOPIC]
    os.environ.update({
        "FLEETFORGE_LAKEHOUSE_TELEMETRY_TOPIC": names[TELEMETRY_TOPIC],
        "FLEETFORGE_LAKEHOUSE_ROBOT_EVENTS_TOPIC": names[ROBOT_EVENTS_TOPIC],
        "FLEETFORGE_LAKEHOUSE_FIRMWARE_TOPIC": names[FIRMWARE_TOPIC],
        "FLEETFORGE_LAKEHOUSE_MISSION_EVENTS_TOPIC": names["missions"],
    })  # fmt: skip
    return names


def spark_phase() -> None:
    from fleetforge.config import get_settings
    from fleetforge.kafka.schemas import register, registry_client, subject_for
    from fleetforge.lakehouse import fixtures as fx

    evidence: dict[str, Any] = {"started_at": datetime.now(UTC), "seed": SEED, "robots": ROBOTS,
                                "ticks": TICKS, "runs": RUNS}  # fmt: skip
    gid = uuid.uuid4().hex[:8]
    job(evidence, "drop test catalog tables", __file__, "drop-tables")
    topics = make_topics(gid)
    client = registry_client(get_settings().schema_registry_url)
    ids = {rev: register(client, subject_for(topics["telemetry"]), rev) for rev in (1, 2)}
    remap = {fx.SCHEMA_V1: ids[1], fx.SCHEMA_V2: ids[2], fx.UNKNOWN_THEN_FOUND: MISSING_SCHEMA_ID}
    corpus = fx.random_corpus(SEED, robots=ROBOTS, ticks=TICKS, runs=RUNS)
    evidence.update(gate_id=gid, topics=topics, schema_ids=ids, published={}, builds=[])

    bronze = "fleetforge.lakehouse.jobs.bronze_ingest"
    silver = "fleetforge.lakehouse.jobs.silver_build"
    release = "fleetforge.lakehouse.jobs.release"
    for run in range(RUNS):
        evidence["published"][run] = publish(corpus, run, topics, remap)
        evidence[f"bronze_run_{run}"] = job(evidence, f"bronze run {run}", bronze)
        if run == 1:  # SIGKILL mid-build, after two tables committed and before events
            job(evidence, "silver killed before events", __file__, "kill-silver",
                expect_kill=True)  # fmt: skip
        evidence["builds"].append(job(evidence, f"silver run {run}", silver))
        if run == RUNS - 2:
            evidence["release_old"] = job(evidence, "release gate-r1", release, "create",
                                          "--release-id", "gate-r1", "--seed", "g1")  # fmt: skip

    evidence["replay"] = job(evidence, "bronze replay from earliest", bronze, "--replay")
    assert evidence["replay"]["new_rows"] == 0
    evidence["silver_rerun"] = job(evidence, "silver rerun (no new data)", silver)
    assert evidence["silver_rerun"]["status"] == "unchanged"
    again = job(evidence, "release gate-r1 again", release, "create", "--release-id", "gate-r1",
                "--seed", "g1")  # fmt: skip
    assert again["status"] == "exists"
    reproduced = job(evidence, "reproduce gate-r1", release, "reproduce", "--release-id", "gate-r1")
    assert reproduced["match"]
    evidence["reproduce_after_new_builds"] = reproduced

    evidence["incremental_digest"] = job(evidence, "digest incremental Silver", __file__, "digest")
    evidence["full_build"] = job(evidence, "silver full rebuild", silver, "--full")
    evidence["full_digest"] = job(evidence, "digest full Silver + Python reference", __file__,
                                  "digest", "--reference")  # fmt: skip
    inc, full = evidence["incremental_digest"], evidence["full_digest"]
    assert inc["events"] == full["events"] == full["reference_events"], (inc, full)
    assert inc["outcomes"] == full["outcomes"] == full["reference_outcomes"], (inc, full)

    evidence["release_new"] = job(evidence, "release gate-r2", release, "create",
                                  "--release-id", "gate-r2", "--seed", "g2")  # fmt: skip
    evidence["maintenance"] = job(evidence, "maintenance (expire all untagged)",
                                  "fleetforge.lakehouse.jobs.maintenance", "--keep-days", "0",
                                  "--retain-last", "2")  # fmt: skip
    for rid in ("gate-r1", "gate-r2"):
        evidence[f"reproduce_{rid}_after_expiry"] = job(
            evidence, f"reproduce {rid} after expiry", release, "reproduce", "--release-id", rid
        )
        assert evidence[f"reproduce_{rid}_after_expiry"]["match"]
    evidence["reconcile"] = job(evidence, "reconcile incl. independent Kafka count",
                                "fleetforge.lakehouse.jobs.reconcile", "--kafka-bootstrap",
                                get_settings().kafka_bootstrap)  # fmt: skip
    assert all(r["passed"] for r in evidence["reconcile"]), evidence["reconcile"]
    evidence["spark_finished_at"] = datetime.now(UTC)
    save(evidence)
    dbt_vars = {"telemetry_topic": topics["telemetry"]}
    EVIDENCE.with_name("gate-a-vars.json").write_text(json.dumps(dbt_vars))
    reopen = json.dumps({**dbt_vars, "reopen": True})
    EVIDENCE.with_name("gate-a-vars-reopen.json").write_text(reopen)


def drop_tables() -> None:
    from fleetforge.lakehouse.jobs.common import session
    from fleetforge.lakehouse.tables import ALL_TABLES, CATALOG

    spark = session("gate-a-reset")
    for t in ALL_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{t.qualified} PURGE")
    print(json.dumps({"dropped": len(ALL_TABLES)}))


def kill_silver() -> None:
    from fleetforge.lakehouse.jobs import silver_build as sj
    from fleetforge.lakehouse.jobs.common import session
    from fleetforge.lakehouse.settings import LakehouseSettings

    lake = LakehouseSettings()

    def die(stage: str) -> None:
        if stage == "before_events":
            os.kill(os.getpid(), signal.SIGKILL)  # the JVM goes with its Python driver

    sj.build_silver(session("gate-a-kill", lake), fault=die, telemetry_topic=lake.telemetry_topic,
                    reference_topics={"firmware": lake.firmware_topic,
                                      "robots": lake.robot_events_topic})  # fmt: skip


def _plain(v: Any) -> Any:
    if isinstance(v, bytearray):
        return bytes(v)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)  # session and container are UTC
    if isinstance(v, list):
        return [_plain(x) for x in v]
    if hasattr(v, "asDict"):
        return {k: _plain(x) for k, x in v.asDict().items()}
    return v


def _digest(rows: list[dict], key) -> str:
    lines = sorted((key(r), json.dumps({k: v for k, v in r.items() if k != "silver_build_id"},
                                       sort_keys=True, default=str)) for r in rows)  # fmt: skip
    return hashlib.sha256("\n".join(x for _, x in lines).encode()).hexdigest()


def digest(with_reference: bool) -> None:
    """Digests of Silver at the newest build and, optionally, of the Python reference."""
    from fleetforge.lakehouse import gold
    from fleetforge.lakehouse.bronze import coordinates
    from fleetforge.lakehouse.jobs.common import fq, session
    from fleetforge.lakehouse.reference import build_reference
    from fleetforge.lakehouse.settings import LakehouseSettings
    from fleetforge.lakehouse.silver import (
        EVENT_COLUMNS,
        OUTCOME_COLUMNS,
        build,
        parse_writer_schemas,
        prepare,
    )
    from fleetforge.lakehouse.tables import (
        BRONZE_KAFKA_RECORDS,
        BRONZE_REGISTRY_SCHEMAS,
        SILVER_BUILD_LOG,
        SILVER_TELEMETRY_EVENTS,
        SILVER_TELEMETRY_RECORD_OUTCOMES,
    )

    lake = LakehouseSettings()
    spark = session("gate-a-digest", lake)
    b = spark.table(fq(SILVER_BUILD_LOG)).orderBy("committed_at", ascending=False).first()

    def rows(table, snap, columns):
        df = spark.sql(f"SELECT * FROM {fq(table)} VERSION AS OF {snap}").select(*columns)
        return [{k: _plain(v) for k, v in r.asDict().items()} for r in df.toLocalIterator()]

    def by_event(e):
        return e["event_id"]

    events = rows(SILVER_TELEMETRY_EVENTS, b["events_snapshot_id"], EVENT_COLUMNS)
    outcomes = rows(SILVER_TELEMETRY_RECORD_OUTCOMES, b["outcomes_snapshot_id"], OUTCOME_COLUMNS)
    out = {"build_id": b["build_id"], "mode": b["mode"], "events_rows": len(events),
           "outcome_rows": len(outcomes), "events": _digest(events, by_event),
           "outcomes": _digest(outcomes, coordinates)}  # fmt: skip
    del events, outcomes
    if with_reference:
        bronze = [{k: _plain(v) for k, v in r.asDict().items()} for r in spark.sql(
            f"SELECT * FROM {fq(BRONZE_KAFKA_RECORDS)} VERSION AS OF {b['bronze_snapshot_id']}"
        ).toLocalIterator()]  # fmt: skip
        archive = [r.asDict() for r in spark.table(fq(BRONZE_REGISTRY_SCHEMAS)).collect()]
        spark.stop()  # free the JVM before the Python reference runs
        wanted = (lake.telemetry_topic, lake.firmware_topic, lake.robot_events_topic)
        by_topic = {t: [r for r in bronze if r["source_topic"] == t] for t in wanted}
        ref = build_reference(by_topic[lake.firmware_topic], by_topic[lake.robot_events_topic])
        writers = parse_writer_schemas(archive)
        tel = by_topic[lake.telemetry_topic]
        want = build([prepare(r, writers, ref) for r in tel], build_id=b["build_id"])
        out.update(reference_events=_digest(want.events, by_event),
                   reference_outcomes=_digest(want.outcomes, coordinates),
                   bronze_rows=len(bronze), telemetry_rows=len(tel))  # fmt: skip
        topic_of = {tel[0]["kafka_topic_id"]: lake.telemetry_topic}
        reference = {
            "build_id": b["build_id"],
            "robot_daily_telemetry": gold.daily_telemetry(want.events, want.outcomes, b["build_id"],
                                                          gold.key_robots(tel)),
            "pipeline_daily_quality": gold.pipeline_quality(want.outcomes, topic_of, b["build_id"]),
            "pipeline_daily_rates": gold.pipeline_rates(tel, want.outcomes, b["build_id"]),
        }  # fmt: skip
        REFERENCE.write_text(json.dumps(reference, default=str))
        out["final_days"] = sum(r["is_final"] for r in reference["robot_daily_telemetry"])
    print(json.dumps(out, default=str))


# --------------------------------------------------------------------------- Trino phase


def trino_phase(stage: str) -> None:
    import trino

    evidence = load()
    want = json.loads(REFERENCE.read_text())
    cur = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=8080,
                              user="gate-a", catalog="lake_test").cursor()  # fmt: skip

    def query(sql: str) -> list[dict]:
        start = time.monotonic()
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        out = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
        evidence.setdefault("trino_queries", []).append(
            {"stage": stage, "sql": sql[:80], "seconds": round(time.monotonic() - start, 2)}
        )
        return out

    keys = {"robot_daily_telemetry": ("observed_date", "robot_id"),
            "pipeline_daily_quality": ("ingest_date", "source_topic", "outcome", "reason_code"),
            "pipeline_daily_rates": ("ingest_date", "source_topic")}  # fmt: skip
    tampered = evidence.get("tampered")
    mismatches = {}
    for table, key in keys.items():
        got = {tuple(str(r[k]) for k in key): r for r in query(f"SELECT * FROM gold.{table}")}
        ref = {tuple(str(r[k]) for k in key): r for r in want[table]}
        bad = [k for k in ref.keys() | got.keys() if k not in got or k not in ref
               or not _row_equal(got[k], ref[k])]  # fmt: skip
        if stage == "frozen" and table == "robot_daily_telemetry":
            k = tuple(tampered)
            assert k in bad and got[k]["events"] == ref[k]["events"] + TAMPER, "final day changed"
            bad.remove(k)
        mismatches[table] = len(bad)
        assert not bad, (table, bad[:5])
    evidence.setdefault("gold_checks", {})[stage] = mismatches
    if stage == "first":
        [final] = query("SELECT observed_date, robot_id FROM gold.robot_daily_telemetry "
                        "WHERE is_final ORDER BY 1, 2 LIMIT 1")  # fmt: skip
        cur.execute(f"UPDATE gold.robot_daily_telemetry SET events = events + {TAMPER} "
                    f"WHERE observed_date = DATE '{final['observed_date']}' "
                    f"AND robot_id = '{final['robot_id']}'")  # fmt: skip
        cur.fetchall()
        evidence["tampered"] = [str(final["observed_date"]), final["robot_id"]]
        counts = query("SELECT count(*) AS rows, count_if(is_final) AS final "
                       "FROM gold.robot_daily_telemetry")  # fmt: skip
        evidence["gold_rows"] = counts[0]
    save(evidence)
    print(json.dumps({"stage": stage, "mismatches": mismatches}))


def _row_equal(got: dict, want: dict) -> bool:
    for col, value in want.items():
        g = got[col]
        if isinstance(g, float) and isinstance(value, float):
            if abs(g - value) > 1e-9 * max(1.0, abs(value)):
                return False
        elif isinstance(g, datetime | date):
            text = g.isoformat().replace("T", " ")
            if text != str(value).replace("T", " ") and not _same_instant(g, value):
                return False
        elif g != value:
            return False
    return True


def _same_instant(g: Any, value: str) -> bool:
    try:
        return isinstance(g, datetime) and g == datetime.fromisoformat(value)
    except ValueError:
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["spark", "trino", "drop-tables", "kill-silver", "digest"])
    ap.add_argument("--stage", default="first", choices=["first", "frozen", "reopened"])
    ap.add_argument("--reference", action="store_true")
    args = ap.parse_args()
    {"spark": spark_phase, "drop-tables": drop_tables, "kill-silver": kill_silver,
     "trino": lambda: trino_phase(args.stage),
     "digest": lambda: digest(args.reference)}[args.phase]()  # fmt: skip


if __name__ == "__main__":
    main()
