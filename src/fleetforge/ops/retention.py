"""Outbox retention.  python -m fleetforge.ops.retention [--days 7] [--dry-run]

Deleting outbox rows is invisible to consumers (the publication publishes INSERTs
only), but it is only *safe* once Debezium has read them. The guard refuses to run
while the outbox replication slot is inactive or more than --max-lag-mb behind:
in that state, rows may not have been captured yet. Rows are deleted in bounded
chunks to keep transactions and WAL small.
"""

from __future__ import annotations

import argparse
import json
import sys

from sqlalchemy import text

from fleetforge.config import get_settings
from fleetforge.db.engine import make_engine

SLOT = "fleetforge_outbox"


def run(days: int, max_lag_mb: int, dry_run: bool, chunk: int = 5000) -> dict:
    engine = make_engine(get_settings())
    with engine.connect() as c:
        slot_sql = text(
            "SELECT active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag"
            " FROM pg_replication_slots WHERE slot_name = :s"
        )
        slot = c.execute(slot_sql, {"s": SLOT}).mappings().first()
        count_sql = text(
            "SELECT count(*) FROM fleet.outbox_events"
            " WHERE created_at < now() - make_interval(days => :d)"
        )
        candidates = c.execute(count_sql, {"d": days}).scalar_one()
    if slot is None or not slot["active"] or slot["lag"] > max_lag_mb * 2**20:
        return {
            "deleted": 0,
            "candidates": candidates,
            "refused": True,
            "reason": f"slot {SLOT} missing, inactive or lagging",
            "slot": dict(slot or {}),
        }
    delete_sql = text(
        "DELETE FROM fleet.outbox_events WHERE event_id IN (SELECT event_id"
        " FROM fleet.outbox_events WHERE created_at < now() - make_interval(days => :d)"
        " LIMIT :n)"
    )
    deleted = 0
    while not dry_run:
        with engine.begin() as c:
            n = c.execute(delete_sql, {"d": days, "n": chunk}).rowcount
        deleted += n
        if n < chunk:
            break
    if not dry_run:  # heartbeats only exist to move the slots; keep one day
        with engine.begin() as c:
            c.execute(
                text("DELETE FROM fleet.cdc_heartbeat WHERE beat_at < now() - interval '1 day'")
            )
    return {"deleted": deleted, "candidates": candidates, "refused": False, "dry_run": dry_run}


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m fleetforge.ops.retention")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--max-lag-mb", type=int, default=64)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    out = run(a.days, a.max_lag_mb, a.dry_run)
    print(json.dumps(out, default=str))
    return 1 if out["refused"] else 0


if __name__ == "__main__":
    sys.exit(main())
