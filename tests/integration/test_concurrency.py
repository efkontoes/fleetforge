"""Acceptance scenario 4: concurrent writes.

Most tests here force an exact interleaving instead of hoping threads collide:
transaction A writes without committing, transaction B is started and we wait
until PostgreSQL reports B as blocked on A's lock, then A commits. This makes
the race deterministic and the test meaningful on any machine.
"""

from __future__ import annotations

import json
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text

from fleetforge.contracts.api import (
    BootRegistration,
    MissionCreate,
    MissionStatus,
    MissionTransition,
    RobotRegistration,
    TelemetryOutcome,
)
from fleetforge.contracts.telemetry import TelemetryEventV1
from fleetforge.db import repository as repo
from fleetforge.services import missions, robots, telemetry
from fleetforge.services.errors import Conflict

SKEW = timedelta(minutes=5)


def make_robot(db: Engine) -> tuple[UUID, UUID]:
    robot_id, boot_id = uuid4(), uuid4()
    robots.register_robot(
        db, RobotRegistration(robot_id=robot_id, robot_model="AMR-100", firmware_version="1.0.0")
    )
    robots.register_boot(db, robot_id, BootRegistration(boot_id=boot_id).boot_id)
    return robot_id, boot_id


def event(robot_id: UUID, boot_id: UUID, seq: int) -> TelemetryEventV1:
    return TelemetryEventV1.model_validate_json(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": str(uuid4()),
                "robot_id": str(robot_id),
                "boot_id": str(boot_id),
                "sequence_number": seq,
                "observed_at": datetime.now(UTC).isoformat(),
                "battery_pct": 50.0,
                "x_m": float(seq),
                "y_m": 0.0,
            }
        )
    )


def held_seq(db: Engine, robot_id: UUID) -> tuple[int, int]:
    with db.connect() as c:
        row = c.execute(
            text(
                "SELECT sequence_number, state_version"
                " FROM fleet.robot_latest_state WHERE robot_id = :r"
            ),
            {"r": robot_id},
        ).one()
    return row.sequence_number, row.state_version


def wait_until_blocked(db: Engine, pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    with db.connect() as c:
        while time.monotonic() < deadline:
            blocked = c.execute(
                text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}
            ).scalar_one()
            if blocked:
                return
            time.sleep(0.01)
    raise AssertionError(f"backend {pid} never blocked on a lock")


def run_blocked(db: Engine, fn) -> tuple[threading.Thread, dict, int]:
    """Open a connection, start fn(conn) in a thread inside a transaction, return its pid."""
    conn = db.connect()
    pid = conn.execute(text("SELECT pg_backend_pid()")).scalar_one()
    conn.commit()
    out: dict = {}

    def target():
        try:
            with conn.begin():
                out["result"] = fn(conn)
        except Exception as exc:  # surfaced by the test
            out["error"] = exc
        finally:
            conn.close()

    t = threading.Thread(target=target)
    t.start()
    return t, out, pid


# ------------------------------------------------------------- deterministic interleavings


@pytest.mark.parametrize(
    ("first", "second", "second_applies"),
    [
        (102, 101, False),  # newer holds the lock; older must not overwrite after it commits
        (101, 102, True),  # older holds the lock; newer must still win after it commits
    ],
)
@pytest.mark.parametrize("row_exists", [True, False], ids=["update-path", "insert-path"])
def test_conditional_upsert_under_forced_race(db, first, second, second_applies, row_exists):
    robot_id, boot_id = make_robot(db)
    if row_exists:
        with db.begin() as c:
            repo.apply_telemetry(c, event(robot_id, boot_id, 100), 1, datetime.now(UTC))

    a = db.connect()
    tx_a = a.begin()
    assert (
        repo.apply_telemetry(a, event(robot_id, boot_id, first), 1, datetime.now(UTC)) is not None
    )

    t, out, pid_b = run_blocked(
        db,
        lambda c: repo.apply_telemetry(c, event(robot_id, boot_id, second), 1, datetime.now(UTC)),
    )
    wait_until_blocked(db, pid_b)  # B is now waiting on A's row (or speculative insert) lock
    tx_a.commit()
    a.close()
    t.join(5)

    assert "error" not in out, out.get("error")
    assert (out["result"] is not None) is second_applies
    assert held_seq(db, robot_id)[0] == 102


def naive_apply(conn, ev: TelemetryEventV1) -> bool:
    """The obvious implementation: check in Python, then write. DO NOT USE."""
    current = conn.execute(
        text("SELECT sequence_number FROM fleet.robot_latest_state WHERE robot_id = :r"),
        {"r": ev.robot_id},
    ).scalar_one()
    if ev.sequence_number <= current:
        return False
    conn.execute(
        text("UPDATE fleet.robot_latest_state SET sequence_number = :s WHERE robot_id = :r"),
        {"s": ev.sequence_number, "r": ev.robot_id},
    )
    return True


def test_naive_read_then_write_loses_the_newer_update(db):
    """Demonstrates the bug the conditional upsert exists to prevent.

    A and B both read seq 100. A writes 102 and commits. B's check (101 > 100) was
    true when it ran, so B writes 101 and commits. The newest state is lost.
    """
    robot_id, boot_id = make_robot(db)
    with db.begin() as c:
        repo.apply_telemetry(c, event(robot_id, boot_id, 100), 1, datetime.now(UTC))

    with db.connect() as a, db.connect() as b:
        tx_a, tx_b = a.begin(), b.begin()
        # Step 1: B reads seq 100 (A will read it too, inside naive_apply).
        assert (
            b.execute(
                text("SELECT sequence_number FROM fleet.robot_latest_state WHERE robot_id = :r"),
                {"r": robot_id},
            ).scalar_one()
            == 100
        )
        # Step 2: A checks and writes 102, commits.
        assert naive_apply(a, event(robot_id, boot_id, 102))
        tx_a.commit()
        # Step 3: B decided "101 > 100" from its earlier read; it now writes.
        b.execute(
            text("UPDATE fleet.robot_latest_state SET sequence_number = 101 WHERE robot_id = :r"),
            {"r": robot_id},
        )
        tx_b.commit()

    assert held_seq(db, robot_id)[0] == 101  # the bug: 102 was overwritten by an older event


# ------------------------------------------------------------- stress (many real threads)


def test_many_threads_shuffled_sequences_converge_to_newest(db):
    robot_id, boot_id = make_robot(db)
    seqs = list(range(1, 301)) * 2  # every event delivered twice
    rng = random.Random(7)
    rng.shuffle(seqs)
    events = {s: event(robot_id, boot_id, s) for s in set(seqs)}
    bodies = [events[s].model_dump_json().encode() for s in seqs]

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda b: telemetry.ingest(db, b, datetime.now(UTC), SKEW), bodies))

    outcomes = Counter(r.outcome for r in results)
    seq, state_version = held_seq(db, robot_id)
    assert seq == 300
    # Every APPLIED response corresponds to exactly one committed state version.
    assert state_version == outcomes[TelemetryOutcome.APPLIED]
    assert set(outcomes) <= {
        TelemetryOutcome.APPLIED,
        TelemetryOutcome.DUPLICATE,
        TelemetryOutcome.STALE,
    }
    assert sum(outcomes.values()) == 600


def test_concurrent_boot_registrations_get_distinct_consecutive_numbers(db):
    robot_id, _ = make_robot(db)  # boot 1
    boot_ids = [uuid4() for _ in range(12)]
    with ThreadPoolExecutor(max_workers=12) as pool:
        numbers = list(pool.map(lambda b: robots.register_boot(db, robot_id, b)[0], boot_ids))
    assert sorted(numbers) == list(range(2, 14))


# ------------------------------------------------------------- missions


def _mission(db, robot_id) -> UUID:
    mission_id = uuid4()
    missions.create_mission(db, MissionCreate(mission_id=mission_id, robot_id=robot_id))
    return mission_id


def test_two_concurrent_assignments_to_one_robot_only_one_wins(db):
    robot_id, _ = make_robot(db)
    m1, m2 = _mission(db, robot_id), _mission(db, robot_id)

    def assign(mission_id):
        try:
            missions.transition_mission(
                db,
                mission_id,
                MissionTransition(
                    transition_id=uuid4(), to_status=MissionStatus.ASSIGNED, expected_version=0
                ),
            )
            return "ok"
        except Conflict as exc:
            return exc.code

    for _ in range(10):  # repeat to give the unforced race many chances
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = sorted(pool.map(assign, [m1, m2]))
        assert results == ["ROBOT_BUSY", "ok"]
        with db.begin() as c:  # reset both to PENDING for the next round
            c.execute(
                text("UPDATE fleet.missions SET status='PENDING', version=0 WHERE robot_id=:r"),
                {"r": robot_id},
            )
            c.execute(
                text("DELETE FROM fleet.outbox_events WHERE event_type='MissionStatusChanged'")
            )
            c.execute(text("DELETE FROM fleet.mission_events WHERE mission_version > 0"))


def test_concurrent_retries_of_one_transition_commit_once(db):
    robot_id, _ = make_robot(db)
    mission_id = _mission(db, robot_id)
    req = MissionTransition(
        transition_id=uuid4(), to_status=MissionStatus.ASSIGNED, expected_version=0
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(lambda _: missions.transition_mission(db, mission_id, req), range(8))
        )

    assert {r.version for r in results} == {1}
    assert sum(not r.replayed for r in results) == 1
    with db.connect() as c:
        n = c.execute(
            text("SELECT count(*) FROM fleet.outbox_events WHERE event_id = :e"),
            {"e": req.transition_id},
        ).scalar_one()
    assert n == 1


def test_retry_racing_its_original_is_replayed_not_reported_as_conflict(db, monkeypatch):
    """Forces the interleaving that made the stress test above fail intermittently:
    the retry passes its idempotency check, THEN the original request commits, THEN
    the retry reads the mission and sees an unexpected version."""
    robot_id, _ = make_robot(db)
    mission_id = _mission(db, robot_id)
    req = MissionTransition(
        transition_id=uuid4(), to_status=MissionStatus.ASSIGNED, expected_version=0
    )
    real_replay = missions._replay
    calls = 0

    def replay_then_original_commits(conn, mid, r):
        nonlocal calls
        result = real_replay(conn, mid, r)
        calls += 1
        if calls == 1:  # the retry's first check has just found nothing...
            original = missions.transition_mission(db, mid, r)  # ...now the original commits
            assert original.replayed is False
        return result

    monkeypatch.setattr(missions, "_replay", replay_then_original_commits)
    retry = missions.transition_mission(db, mission_id, req)

    assert (retry.version, retry.replayed) == (1, True)
