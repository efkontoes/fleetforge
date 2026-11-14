"""Disaster-recovery checks run inside the Spark container.

record  -> /evidence/dr-expected.json: rows and current snapshot of every table
copy-out / copy-in -> object store <-> /backup/objects (S3 API, layout-independent)
verify  -> the restored lake has the same rows and snapshots, every release reproduces,
           and reconciliation passes
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

EXPECTED = Path("/evidence/dr-expected.json")
OBJECTS = "/backup/objects"


def state(spark) -> dict:
    from fleetforge.lakehouse.jobs.common import current_snapshot_id, fq
    from fleetforge.lakehouse.tables import ALL_TABLES

    out = {}
    for t in ALL_TABLES:
        if spark.catalog.tableExists(fq(t)):
            out[t.qualified] = {
                "rows": spark.table(fq(t)).count(),
                "snapshot": current_snapshot_id(spark, t),
            }
    return out


def copy(direction: str) -> None:
    from pyarrow import fs

    from fleetforge.lakehouse.jobs.common import object_store, warehouse_bucket
    from fleetforge.lakehouse.settings import LakehouseSettings

    lake = LakehouseSettings()
    s3, local = object_store(lake), fs.LocalFileSystem()
    bucket = warehouse_bucket(lake)
    if direction == "out":
        fs.copy_files(bucket, OBJECTS, source_filesystem=s3, destination_filesystem=local)
    else:
        fs.copy_files(OBJECTS, bucket, source_filesystem=local, destination_filesystem=s3)
    files = [f for f in local.get_file_info(fs.FileSelector(OBJECTS, recursive=True)) if f.is_file]
    print(json.dumps({"objects": len(files), "bytes": sum(f.size for f in files)}))


def main() -> None:
    from fleetforge.lakehouse.jobs.common import session

    action = sys.argv[1]
    if action in ("copy-out", "copy-in"):
        copy(action.removeprefix("copy-"))
        return
    spark = session("dr-check")
    if action == "record":
        EXPECTED.write_text(json.dumps(state(spark), indent=2))
        print(EXPECTED.read_text())
        return
    from fleetforge.lakehouse.jobs import reconcile, release
    from fleetforge.lakehouse.jobs.common import fq
    from fleetforge.lakehouse.tables import GOLD_TRAINING_DATASET_RELEASES

    expected, actual = json.loads(EXPECTED.read_text()), state(spark)
    assert actual == expected, {"expected": expected, "actual": actual}
    ids = [
        r[0] for r in spark.table(fq(GOLD_TRAINING_DATASET_RELEASES)).select("release_id").collect()
    ]
    reproduced = {rid: release.reproduce_release(spark, rid)["match"] for rid in ids}
    assert all(reproduced.values()), reproduced
    from fleetforge.lakehouse.settings import LakehouseSettings

    checks = reconcile.reconcile(spark, telemetry_topic=LakehouseSettings().telemetry_topic)
    assert all(c["passed"] for c in checks), [c for c in checks if not c["passed"]]
    print(
        json.dumps(
            {
                "tables": len(actual),
                "releases_reproduced": reproduced,
                "reconciliation_checks": len(checks),
            }
        )
    )


if __name__ == "__main__":
    main()
