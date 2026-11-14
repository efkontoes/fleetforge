"""Incremental Silver: recompute only what a change can reach (ADR 0017).

A changed record can only affect rows that share its robot (slots, out-of-order order),
its event_id (duplicates and conflicts) or a correction link, so the update recomputes
the closure of those keys and must equal a full rebuild.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from fleetforge.lakehouse.bronze import coordinates
from fleetforge.lakehouse.reference import Reference
from fleetforge.lakehouse.silver import Prepared, build, parse_writer_schemas, prepare


def effective_archive(archive: Iterable[Mapping[str, Any]]) -> dict[int, tuple[str, str]]:
    """{schema_id: (status, schema hash)} with FOUND winning over NOT_FOUND."""
    out: dict[int, tuple[str, str]] = {}
    for r in archive:
        sid = int(r["schema_id"])
        digest = hashlib.sha256((r["schema_text"] or "").encode()).hexdigest()
        if r["status"] == "FOUND" or sid not in out:
            out[sid] = (r["status"], digest)
    return out


def schema_changes(old: Iterable[Mapping], new: Iterable[Mapping]) -> set[int]:
    """Schema ids whose archived answer changed, e.g. NOT_FOUND turning into FOUND."""
    before, after = effective_archive(old), effective_archive(new)
    return {sid for sid in before.keys() | after.keys() if before.get(sid) != after.get(sid)}


def reference_changes(old: Reference, new: Reference) -> set[str]:
    """Robots whose prepared rows depend on reference data that changed."""
    robots = {r for r in old.robots.keys() | new.robots.keys()
              if old.robot_model(r) != new.robot_model(r)}  # fmt: skip

    def units(ref: Reference, key: tuple[str, str]) -> tuple[str, str] | None:
        c = ref.firmware.get(key)
        return (c.battery_unit, c.position_unit) if c else None

    models = {key[0] for key in old.firmware.keys() | new.firmware.keys()
              if units(old, key) != units(new, key)}  # fmt: skip
    for ref in (old, new):
        robots |= {r for r, reg in ref.robots.items() if reg.robot_model in models}
    return robots


def link_keys(rows: Iterable[Mapping[str, Any]]) -> set[str]:
    return {k for r in rows for k in (r["event_id"], r["correction_of"]) if k}


def closure(rows: list[Mapping[str, Any]], changed: list[Mapping[str, Any]]) -> tuple[set, set]:
    """Robots and link keys the set stage must recompute for the changed rows."""
    robots = {r["robot_id"] for r in changed if r["robot_id"]}
    while True:
        keys = link_keys(changed) | link_keys(r for r in rows if r["robot_id"] in robots)
        linked = {r["robot_id"] for r in rows if r["robot_id"]
                  and (r["event_id"] in keys or r["correction_of"] in keys)}  # fmt: skip
        if linked <= robots:
            return robots, keys
        robots |= linked


def in_scope(row: Mapping[str, Any], robots: set, keys: set, changed: set) -> bool:
    if row["robot_id"]:
        return row["robot_id"] in robots
    return row["event_id"] in keys or row["coords"] in changed


def keys_of(p: Prepared) -> dict[str, Any]:
    return {"coords": coordinates(p.row), "robot_id": p.robot_id, "event_id": p.event_id,
            "correction_of": p.event.correction_of if p.event else None}  # fmt: skip


@dataclass
class SilverState:
    """The Python reference of Silver's tables, kept between incremental builds."""

    bronze: dict[tuple, dict] = field(default_factory=dict)
    prepared: dict[tuple, Prepared] = field(default_factory=dict)
    events: dict[str, dict] = field(default_factory=dict)
    outcomes: dict[tuple, dict] = field(default_factory=dict)
    reference: Reference | None = None
    archive: list[dict] = field(default_factory=list)


def update(state: SilverState, new_bronze: list[dict], reference: Reference,
           archive: list[dict], build_id: str) -> dict[str, int]:  # fmt: skip
    """Apply one incremental build to `state` and return what it recomputed."""
    writers = parse_writer_schemas(archive)
    for row in new_bronze:
        state.bronze[coordinates(row)] = row
    if state.reference is None:
        redo: list[tuple] = []
    else:
        robots = reference_changes(state.reference, reference)
        ids = schema_changes(state.archive, archive)
        redo = [c for c, p in state.prepared.items()
                if p.robot_id in robots or p.row["value_schema_id"] in ids]  # fmt: skip
    changed = [coordinates(r) for r in new_bronze] + redo
    for c in changed:
        state.prepared[c] = prepare(state.bronze[c], writers, reference)

    rows = [keys_of(p) for p in state.prepared.values()]
    changed_set = set(changed)
    robots, keys = closure(rows, [r for r in rows if r["coords"] in changed_set])
    scope = [p for p, r in zip(state.prepared.values(), rows, strict=True)
             if in_scope(r, robots, keys, changed_set)]  # fmt: skip
    result = build(scope, build_id=build_id)
    for o in result.outcomes:
        state.outcomes[coordinates(o)] = o
    state.events = {e: row for e, row in state.events.items() if row["robot_id"] not in robots}
    state.events.update({e["event_id"]: e for e in result.events})
    state.reference, state.archive = reference, list(archive)
    return {"new": len(new_bronze), "reprepared": len(redo), "robots": len(robots),
            "scope": len(scope)}  # fmt: skip
