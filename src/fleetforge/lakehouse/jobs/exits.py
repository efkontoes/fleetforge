"""Exit codes the scheduler acts on: retry a temporary failure, stop on a permanent one."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("fleetforge.lakehouse.exits")

TEMPORARY = 75  # EX_TEMPFAIL: run again later
PERMANENT = 65  # EX_DATAERR: a person must decide before a retry can succeed

# Failures that pass on their own: services down, a lease or commit race lost.
TEMPORARY_MARKERS = (
    "RegistryUnavailable", "LeaseUnavailable", "SchemaArchiveIncomplete", "ConcurrentWrite",
    "CommitFailedException", "ValidationException", "Connection refused", "ConnectException",
    "UnknownHostException", "SdkClientException", "TimeoutException", "OperationalError",
)  # fmt: skip
# Failures that repeat until someone acts: a recreated topic, a clashing release.
PERMANENT_MARKERS = ("BronzePositionError", "ReleaseError", "ExportMismatch")


def classify(exc: BaseException) -> int:
    text = f"{type(exc).__name__}: {exc}"
    if any(m in text for m in PERMANENT_MARKERS):
        return PERMANENT
    if any(m in text for m in TEMPORARY_MARKERS):
        return TEMPORARY
    return 1  # unknown: the scheduler's normal retry policy applies


def run(main: Callable[[], Any]) -> None:
    """Run main() (a report, or (exit_code, report)), print and record it, then exit."""
    started = datetime.now(UTC)
    job = Path(sys.argv[0]).stem
    try:
        result = main()
        code, report = result if isinstance(result, tuple) else (0, result)
    except SystemExit:
        raise
    except BaseException as exc:
        code, report = classify(exc), {"error": f"{type(exc).__name__}: {exc}"[:2000]}
        log.exception("job failed (exit %s)", code)
    print(json.dumps(report, default=str), flush=True)
    _record(job, code, started, report)
    sys.exit(code)


def _record(job: str, code: int, started: datetime, report: Any) -> None:
    from fleetforge.lakehouse import ops
    from fleetforge.lakehouse.settings import LakehouseSettings

    lake = LakehouseSettings()
    if not lake.record_runs:
        return
    lineage = report.get("lineage") if isinstance(report, dict) else None
    try:
        ops.record(lake.catalog_dsn, job, code, started, report, lineage)
    except Exception as exc:  # monitoring must never fail the job it watches
        log.warning("could not record the run: %s", exc)
