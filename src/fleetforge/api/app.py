"""Fleet API. Handlers translate HTTP <-> services; no business rules live here."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest, multiprocess
from sqlalchemy import Engine, text
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from fleetforge.config import Settings, get_settings
from fleetforge.contracts.api import (
    Boot,
    BootRegistration,
    Mission,
    MissionCreate,
    MissionTransition,
    Robot,
    RobotRegistration,
    RobotState,
    TelemetryOutcome,
    TelemetryResult,
)
from fleetforge.db import repository as repo
from fleetforge.db.engine import make_engine
from fleetforge.observability import DB_UNAVAILABLE, TELEMETRY_LATENCY, configure_logging
from fleetforge.services import missions, robots, telemetry
from fleetforge.services.errors import NotFound, ServiceError

log = logging.getLogger(__name__)

TELEMETRY_STATUS = {
    TelemetryOutcome.APPLIED: 200,
    TelemetryOutcome.DUPLICATE: 200,  # idempotent retry: the client must stop retrying
    TelemetryOutcome.STALE: 200,  # final decision; retrying cannot change it
    TelemetryOutcome.CONFLICT: 409,
    TelemetryOutcome.REJECTED: 422,
}


def create_app(settings: Settings | None = None, engine: Engine | None = None) -> FastAPI:
    settings = settings or get_settings()
    owns_engine = engine is None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        app.state.engine = engine or make_engine(settings)
        yield
        if owns_engine:
            app.state.engine.dispose()

    app = FastAPI(title="FleetForge Fleet API", version="0.1.0", lifespan=lifespan)
    skew = timedelta(seconds=settings.max_clock_skew_s)

    def eng(request: Request) -> Engine:
        return request.app.state.engine

    # ------------------------------------------------------------- error mapping

    @app.exception_handler(ServiceError)
    async def service_error(_: Request, exc: ServiceError) -> JSONResponse:
        return _error(exc.status_code, exc.code, exc.message, exc.details)

    @app.exception_handler(OperationalError)
    @app.exception_handler(InterfaceError)
    @app.exception_handler(PoolTimeoutError)
    async def db_unavailable(_: Request, exc: Exception) -> JSONResponse:
        # Never pretend success: the client gets a retryable status, and because every
        # write path is idempotent, retrying after recovery is safe.
        DB_UNAVAILABLE.inc()
        log.warning("database unavailable", extra={"error": type(exc).__name__})
        resp = _error(503, "DATABASE_UNAVAILABLE", "operational database is unavailable")
        resp.headers["Retry-After"] = "1"
        return resp

    # ------------------------------------------------------------- health

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    def readyz(request: Request) -> dict:
        with eng(request).connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
            # Several uvicorn processes: aggregate their metric files into one view.
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # ------------------------------------------------------------- robots

    @app.post("/v1/robots", response_model=Robot)
    def register_robot(req: RobotRegistration, request: Request, response: Response):
        row, created = robots.register_robot(eng(request), req)
        response.status_code = 201 if created else 200
        return Robot(**row)

    @app.post("/v1/robots/{robot_id}/boots", response_model=Boot)
    def register_boot(robot_id: UUID, req: BootRegistration, request: Request, response: Response):
        boot_number, created = robots.register_boot(eng(request), robot_id, req.boot_id)
        response.status_code = 201 if created else 200
        return Boot(robot_id=robot_id, boot_id=req.boot_id, boot_number=boot_number)

    @app.get("/v1/robots/{robot_id}/state", response_model=RobotState)
    def robot_state(robot_id: UUID, request: Request):
        with eng(request).connect() as conn:
            row = repo.get_robot_state(conn, robot_id)
        if row is None:
            raise NotFound("NO_STATE", f"no accepted telemetry for robot {robot_id}")
        return RobotState(**row, is_fresh=row.age_s <= settings.state_freshness_s)

    # ------------------------------------------------------------- telemetry

    # Takes the raw body on purpose: FastAPI's automatic 422 for a bad body would
    # never reach our rejection audit table.
    @app.post(
        "/v1/telemetry",
        response_model=TelemetryResult,
        responses={409: {"model": TelemetryResult}, 422: {"model": TelemetryResult}},
    )
    async def ingest_telemetry(request: Request) -> JSONResponse:
        if not settings.http_telemetry_enabled:
            return _error(410, "HTTP_TELEMETRY_DISABLED", "publish telemetry to Kafka instead")
        body = await request.body()
        started = time.perf_counter()
        received_at = datetime.now(UTC)
        result = await run_in_threadpool(telemetry.ingest, eng(request), body, received_at, skew)
        TELEMETRY_LATENCY.labels(result.outcome.value).observe(time.perf_counter() - started)
        payload = TelemetryResult(
            outcome=result.outcome,
            event_id=result.event_id,
            reason_code=result.reason_code,
            errors=result.errors,
            state_version=result.state_version,
        )
        return JSONResponse(
            payload.model_dump(mode="json"), status_code=TELEMETRY_STATUS[result.outcome]
        )

    # ------------------------------------------------------------- missions

    @app.post("/v1/missions", response_model=Mission)
    def create_mission(req: MissionCreate, request: Request, response: Response):
        row, created = missions.create_mission(eng(request), req)
        response.status_code = 201 if created else 200
        return Mission(**row)

    @app.get("/v1/missions/{mission_id}", response_model=Mission)
    def get_mission(mission_id: UUID, request: Request):
        return Mission(**missions.get_mission(eng(request), mission_id))

    @app.post("/v1/missions/{mission_id}/transitions")
    def transition_mission(mission_id: UUID, req: MissionTransition, request: Request):
        r = missions.transition_mission(eng(request), mission_id, req)
        return {
            "mission_id": r.mission_id,
            "transition_id": r.transition_id,
            "from_status": r.from_status,
            "to_status": r.to_status,
            "version": r.version,
            "replayed": r.replayed,
        }

    return app


def _error(status: int, code: str, message: str, details: dict | None = None) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, **(details or {})}}, status_code=status
    )


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory fleetforge.api.app:app_factory`."""
    return create_app()
