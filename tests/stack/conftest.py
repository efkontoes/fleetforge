"""Fixtures for tests against the running Compose stack (`make up-full`).

Opt-in: FLEETFORGE_STACK=1. Tests isolate themselves with unique topics, consumer
groups, seeds and robot ids, so they never truncate shared tables and can run
against a stack that also serves other traffic.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import Engine, create_engine, text

from fleetforge.config import Settings
from fleetforge.kafka.retry import retry_transient
from fleetforge.kafka.schemas import register, registry_client, subject_for

ROOT = Path(__file__).resolve().parents[2]
API = os.environ.get("FLEETFORGE_API_URL", "http://localhost:8000")
BOOTSTRAP = os.environ.get("FLEETFORGE_KAFKA_BOOTSTRAP", "127.0.0.1:29092")
REGISTRY = os.environ.get("FLEETFORGE_SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7")
CONNECT = os.environ.get("FLEETFORGE_CONNECT_URL", "http://localhost:8083")

pytestmark = pytest.mark.stack


def pytest_collection_modifyitems(config, items):
    if os.environ.get("FLEETFORGE_STACK") == "1":
        return
    skip = pytest.mark.skip(reason="needs the running stack: FLEETFORGE_STACK=1 (make up-full)")
    for item in items:
        if "tests/stack" in str(item.fspath):
            item.add_marker(skip)


def _env_file() -> dict[str, str]:
    env = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k] = v
    return env


@pytest.fixture(scope="session")
def owner_url() -> str:
    env = {**_env_file(), **os.environ}
    return (f"postgresql+psycopg://{env.get('POSTGRES_USER', 'fleetforge')}:"
            f"{env['POSTGRES_PASSWORD']}@localhost:5432/fleetforge")  # fmt: skip


@pytest.fixture(scope="session")
def stack_db(owner_url) -> Iterator[Engine]:
    eng = create_engine(owner_url, pool_pre_ping=True, pool_size=5)
    yield eng
    eng.dispose()


@pytest.fixture(scope="session")
def stack_settings(owner_url) -> Settings:
    return Settings(database_url=owner_url, kafka_bootstrap=BOOTSTRAP,
                    schema_registry_url=REGISTRY, db_pool_size=4, db_max_overflow=0)  # fmt: skip


@pytest.fixture
def fresh_topic() -> Iterator[str]:
    """A dedicated 6-partition telemetry topic with the v1 schema registered."""
    name = f"telemetry.test.{uuid.uuid4().hex[:10]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    def create():
        try:
            admin.create_topics([NewTopic(name, 6, 1)])[name].result(30)
        except KafkaException as exc:  # a retried request whose first attempt succeeded
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

    retry_transient(create)
    register(registry_client(REGISTRY), subject_for(name), 1)
    yield name
    admin.delete_topics([name])


def admin_with_leaders(topic: str, partitions: list[int], timeout: float = 30) -> AdminClient:
    """A fresh AdminClient whose own metadata already names a leader for `partitions`.

    Leader-based admin requests (DeleteRecords) issued before a new client has that
    metadata failed in CI with _NOENT "No leaders found" (run 43531660032). This is a
    precondition, not a retry: it fails loudly if leaders never become known."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        md = admin.list_topics(topic, timeout=5).topics.get(topic)
        if (
            md is not None
            and md.error is None
            and all(p in md.partitions and md.partitions[p].leader >= 0 for p in partitions)
        ):
            return admin
        time.sleep(0.2)
    raise AssertionError(f"no leader known for {topic} partitions {partitions} after {timeout}s")


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


def wait_until(cond: Callable[[], bool], timeout: float = 60, every: float = 0.5,
               msg: str = "condition") -> float:  # fmt: skip
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if cond():
            return time.monotonic() - start
        time.sleep(every)
    raise AssertionError(f"timed out after {timeout}s waiting for {msg}")


def high_watermarks(topic: str) -> dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"hw-{uuid.uuid4()}"})
    try:
        md = retry_transient(lambda: c.list_topics(topic, timeout=10)).topics[topic]
        return {p: retry_transient(lambda p=p: c.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=10, cached=False))[1]
                for p in md.partitions}  # fmt: skip
    finally:
        c.close()


def ledger_caught_up(db: Engine, group: str, topic: str) -> bool:
    hw = high_watermarks(topic)
    with db.connect() as c:
        rows = dict(c.execute(text("SELECT partition, next_offset FROM fleet.consumer_offsets"
                                   " WHERE consumer_group = :g AND topic = :t"),
                              {"g": group, "t": topic}).all())  # fmt: skip
    return all(rows.get(p, 0) >= h for p, h in hw.items() if h > 0)
