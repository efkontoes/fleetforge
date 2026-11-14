"""Incremental Silver must equal a full rebuild over the same Bronze (Python reference)."""

from __future__ import annotations

import hashlib
import json

import pytest

from fleetforge.lakehouse import fixtures as fx
from fleetforge.lakehouse import incremental as inc
from fleetforge.lakehouse.bronze import coordinates
from fleetforge.lakehouse.reference import FIRMWARE_TOPIC, ROBOT_EVENTS_TOPIC, build_reference
from fleetforge.lakehouse.silver import TELEMETRY_TOPIC, build, parse_writer_schemas, prepare

VOLATILE = {"silver_build_id"}


def canonical(rows, key) -> list[str]:
    out = [json.dumps({k: v for k, v in r.items() if k not in VOLATILE}, sort_keys=True,
                      default=str) for r in rows]  # fmt: skip
    return [s for _, s in sorted(zip([key(r) for r in rows], out, strict=True))]


def digest(rows, key) -> str:
    return hashlib.sha256("\n".join(canonical(rows, key)).encode()).hexdigest()


def reference_upto(runs):
    rows = [r for run in runs for r in run]
    firmware = [r for r in rows if r["source_topic"] == FIRMWARE_TOPIC]
    return build_reference(firmware, [r for r in rows if r["source_topic"] == ROBOT_EVENTS_TOPIC])


def telemetry(run):
    return [r for r in run if r["source_topic"] == TELEMETRY_TOPIC]


def full_build(runs, archive):
    ref = reference_upto(runs)
    writers = parse_writer_schemas(archive)
    prepared = [prepare(r, writers, ref) for run in runs for r in telemetry(run)]
    return build(prepared, build_id="full")


def run_incremental(runs, archives, state=None):
    state = state or inc.SilverState()
    stats = []
    for n, run in enumerate(runs):
        ref = reference_upto(runs[: n + 1])
        stats.append(inc.update(state, telemetry(run), ref, archives[n], f"inc-{n}"))
    return state, stats


def assert_equivalent(state, full):
    def by_event(e):
        return e["event_id"]

    assert digest(state.events.values(), by_event) == digest(full.events, by_event)
    assert digest(state.outcomes.values(), coordinates) == digest(full.outcomes, coordinates)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("runs", [2, 3, 5])
def test_incremental_equals_full_rebuild(seed, runs):
    corpus = fx.random_corpus(seed, runs=runs)
    bronze = fx.corpus_bronze(corpus)
    state, _ = run_incremental(bronze, corpus.archives)
    assert_equivalent(state, full_build(bronze, corpus.archives[-1]))


@pytest.mark.parametrize(("seed", "robots", "runs"), [(6, 10, 4), (85, 16, 3), (93, 6, 3)])
def test_a_reprepared_row_still_reaches_what_its_old_links_reached(seed, robots, runs):
    # A correction re-prepared into a rejection drops its link; its old target must change too.
    corpus = fx.random_corpus(seed, robots=robots, runs=runs)
    bronze = fx.corpus_bronze(corpus)
    state, _ = run_incremental(bronze, corpus.archives)
    assert_equivalent(state, full_build(bronze, corpus.archives[-1]))


def test_the_corpus_exercises_every_kind_of_change():
    seen = set()
    for seed in range(10):
        corpus = fx.random_corpus(seed, runs=4)
        state, stats = run_incremental(fx.corpus_bronze(corpus), corpus.archives)
        seen |= {(o["outcome"], o["reason_code"]) for o in state.outcomes.values()}
        if any(s["reprepared"] for s in stats[1:]):
            seen.add("reprepared")
    for case in [("DUPLICATE", None), ("REJECTED", "EVENT_ID_CONFLICT"),
                 ("REJECTED", "SEQUENCE_CONFLICT"), ("REJECTED", "BATTERY_OUT_OF_RANGE"),
                 ("REJECTED", "MALFORMED_PAYLOAD"), ("ACCEPTED", None), "reprepared"]:  # fmt: skip
        assert case in seen, case


def test_replaying_a_run_changes_nothing():
    corpus = fx.random_corpus(3, runs=3)
    bronze = fx.corpus_bronze(corpus)
    state, _ = run_incremental(bronze, corpus.archives)
    before = (digest(state.events.values(), lambda e: e["event_id"]),
              digest(state.outcomes.values(), coordinates))  # fmt: skip
    ref = reference_upto(bronze)
    inc.update(state, telemetry(bronze[1]), ref, corpus.archives[-1], "replay")
    after = (digest(state.events.values(), lambda e: e["event_id"]),
             digest(state.outcomes.values(), coordinates))  # fmt: skip
    assert after == before


def test_a_crash_after_preparing_is_repaired_by_rerunning(monkeypatch):
    corpus = fx.random_corpus(5, runs=3)
    bronze = fx.corpus_bronze(corpus)
    state, _ = run_incremental(bronze[:2], corpus.archives[:2])
    ref = reference_upto(bronze)
    with monkeypatch.context() as m:
        m.setattr(inc, "build", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash")))
        with pytest.raises(RuntimeError):
            inc.update(state, telemetry(bronze[2]), ref, corpus.archives[2], "crashed")
    inc.update(state, telemetry(bronze[2]), ref, corpus.archives[2], "rerun")
    assert_equivalent(state, full_build(bronze, corpus.archives[2]))


def test_a_narrow_change_recomputes_only_its_robot():
    corpus = fx.random_corpus(7, robots=8, runs=2)
    bronze = fx.corpus_bronze(corpus)
    state, _ = run_incremental(bronze, corpus.archives)
    robot = next(e["robot_id"] for e in state.events.values())
    late = next(r for r in telemetry(bronze[0]) if r["record_key"] == robot.encode())
    extra = {**late, "kafka_offset": 10_000, "ingested_at": late["ingested_at"].replace(year=2027)}
    stats = inc.update(state, [extra], reference_upto(bronze), corpus.archives[-1], "late")
    assert stats["robots"] == 1
    assert stats["scope"] < len(state.prepared) / 4
    assert_equivalent(state, full_build([*bronze, [extra]], corpus.archives[-1]))
