"""Export a dataset release as Parquet files with a manifest, checksums and a data card.

python -m fleetforge.lakehouse.jobs.export --release-id r1 --to /exports
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fleetforge.lakehouse import gold
from fleetforge.lakehouse.jobs import exits
from fleetforge.lakehouse.jobs.common import fq, session
from fleetforge.lakehouse.jobs.release import ReleaseError, _release_row, tag_name
from fleetforge.lakehouse.settings import LakehouseSettings
from fleetforge.lakehouse.tables import GOLD_ROBOT_STATE_OBSERVATIONS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("fleetforge.lakehouse.export")
ROWS_PER_GROUP = 50_000


def _arrow_schema():
    import pyarrow as pa

    types = {
        "STRING": pa.string(),
        "BIGINT": pa.int64(),
        "INT": pa.int32(),
        "DOUBLE": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "TIMESTAMP": pa.timestamp("us", "UTC"),
        "ARRAY<STRING>": pa.list_(pa.string()),
    }
    return pa.schema(
        [pa.field(c.name, types[c.type], c.nullable) for c in GOLD_ROBOT_STATE_OBSERVATIONS.columns]
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def export_release(spark: SparkSession, release_id: str, to: Path) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    release = _release_row(spark, release_id)
    if release is None:
        raise ReleaseError(f"no release {release_id}")
    out = to / release_id
    out.mkdir(parents=True, exist_ok=True)
    df = spark.sql(
        f"SELECT * FROM {fq(GOLD_ROBOT_STATE_OBSERVATIONS)} "
        f"VERSION AS OF '{tag_name(release_id)}' WHERE release_id = '{release_id}'"
    )
    schema = _arrow_schema()
    writers: dict[str, pq.ParquetWriter] = {}
    buffers: dict[str, list[dict]] = {name: [] for name, _ in gold.SPLITS}
    counts = {name: 0 for name, _ in gold.SPLITS}
    digest = hashlib.sha256()

    def flush(split: str) -> None:
        if not buffers[split]:
            return
        if split not in writers:
            writers[split] = pq.ParquetWriter(
                out / f"{split}.parquet", schema, compression="zstd", write_statistics=True
            )
        writers[split].write_table(pa.Table.from_pylist(buffers[split], schema=schema))
        buffers[split].clear()

    for r in df.orderBy("observation_id").toLocalIterator():
        row = r.asDict()
        digest.update(gold.row_line(row).encode() + b"\n")
        buffers[row["split"]].append(row)
        counts[row["split"]] += 1
        if len(buffers[row["split"]]) >= ROWS_PER_GROUP:
            flush(row["split"])
    for split in buffers:
        flush(split)
    for w in writers.values():
        w.close()
    content = digest.hexdigest()
    if content != release["content_sha256"]:
        raise ReleaseError(f"export of {release_id} does not match its recorded content hash")

    files = [
        {
            "path": p.name,
            "sha256": sha256_file(p),
            "bytes": p.stat().st_size,
            "rows": counts[p.stem],
        }
        for p in sorted(out.glob("*.parquet"))
    ]
    manifest = {
        "release_id": release_id,
        "content_sha256": content,
        "files": files,
        "rows": counts,
        "silver_build_id": release["silver_build_id"],
        "silver_events_snapshot_id": release["silver_events_snapshot_id"],
        "feature_schema_version": release["feature_schema_version"],
        "split_seed": release["split_seed"],
        "readiness": release["readiness"],
        "schema": [
            {"name": c.name, "type": c.type, "nullable": c.nullable}
            for c in GOLD_ROBOT_STATE_OBSERVATIONS.columns
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    (out / "DATA_CARD.md").write_text(data_card(release, counts))
    (out / "SHA256SUMS").write_text(
        "".join(
            f"{sha256_file(p)}  {p.name}\n" for p in sorted(out.iterdir()) if p.name != "SHA256SUMS"
        )
    )
    return {"release_id": release_id, "dir": str(out), "content_sha256": content, "files": files}


def data_card(release: dict[str, Any], counts: dict[str, int]) -> str:
    return f"""# Data card: {release["release_id"]}

**Synthetic data.** Every row comes from FleetForge's fleet simulator, not from robots.

| | |
|---|---|
| Release | `{release["release_id"]}`, readiness `{release["readiness"]}` |
| Content SHA-256 | `{release["content_sha256"]}` |
| Silver build | `{release["silver_build_id"]}` |
| Events snapshot | {release["silver_events_snapshot_id"]} |
| Transform / features | `{release["transform_version"]}` / `{release["feature_schema_version"]}` |
| Rows | train {counts["train"]}, validation {counts["validation"]}, test {counts["test"]} |
| Split | hash of episode (`split`) or of robot (`split_robot`) |
| Split seed | `{release["split_seed"]}` |

## What a row is
One current telemetry event of one robot: battery, position, optional heading, motor
temperature and lidar flag, all in canonical units. Episodes are boots. `dt_prev_s`,
`vx_mps` and `vy_mps` are finite differences over the previous consecutive step and
are NULL, with a quality flag, when there is no valid previous step.

## Known limitations
{release["known_limitations"]}

## Intended use
Teaching and testing the pipeline: baselines, leakage checks, drift checks. Not for any
decision about a physical robot.
"""


def main(argv: list[str] | None = None) -> dict[str, Any]:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="fleet-lake-export")
    ap.add_argument("--release-id", required=True)
    ap.add_argument("--to", type=Path, default=Path("/exports"))
    args = ap.parse_args(argv)
    return export_release(
        session("fleetforge-export", LakehouseSettings()), args.release_id, args.to
    )


if __name__ == "__main__":
    exits.run(main)
