"""Telemetry Avro schemas and their lifecycle in the schema registry.

Deployment discipline: producers never auto-register schemas. A schema is
registered (or rejected) by `fleet-schemas register`, which runs as a deployment
step under the subject's BACKWARD compatibility rule. The serializer then only
*looks up* the id of an already-approved schema.

    fleet-schemas register --revision 1        # set BACKWARD + register v1
    fleet-schemas check --revision 3           # dry-run compatibility check
"""

from __future__ import annotations

import argparse
import json
import sys
from functools import cache
from importlib.resources import files

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from fleetforge.config import get_settings

COMPATIBILITY = "BACKWARD"  # new readers must be able to read all older data
REVISIONS = {1: "telemetry_event_v1.avsc", 2: "telemetry_event_v2.avsc",
             3: "telemetry_event_v3_incompatible.avsc"}  # fmt: skip


@cache
def schema_str(revision: int = 1) -> str:
    text = files("fleetforge.contracts").joinpath("avro", REVISIONS[revision]).read_text()
    return json.dumps(json.loads(text))  # canonical whitespace


def subject_for(topic: str) -> str:
    return f"{topic}-value"  # TopicNameStrategy


def registry_client(url: str | None = None) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url or get_settings().schema_registry_url})


def ensure_compatibility(client: SchemaRegistryClient, subject: str) -> None:
    client.set_compatibility(subject_name=subject, level=COMPATIBILITY)


def is_compatible(client: SchemaRegistryClient, subject: str, revision: int) -> bool:
    try:
        return client.test_compatibility(subject, Schema(schema_str(revision), "AVRO"))
    except SchemaRegistryError as exc:
        if exc.http_status_code == 404:  # no versions yet: anything is compatible
            return True
        raise


def register(
    client: SchemaRegistryClient, subject: str, revision: int, *, set_rule: bool = True
) -> int:
    """Returns the global schema id. Raises SchemaRegistryError (409) if incompatible.
    set_rule=False only for the in-memory mock client used by unit tests."""
    if set_rule:
        ensure_compatibility(client, subject)
    return client.register_schema(subject, Schema(schema_str(revision), "AVRO"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fleet-schemas")
    ap.add_argument("action", choices=["register", "check"])
    ap.add_argument("--revision", type=int, default=1, choices=sorted(REVISIONS))
    ap.add_argument("--topic", default=get_settings().telemetry_topic)
    ap.add_argument("--registry", default=None)
    a = ap.parse_args(argv)
    client = registry_client(a.registry)
    subject = subject_for(a.topic)
    if a.action == "check":
        ok = is_compatible(client, subject, a.revision)
        print(json.dumps({"subject": subject, "revision": a.revision, "compatible": ok}))
        return 0 if ok else 1
    try:
        schema_id = register(client, subject, a.revision)
    except SchemaRegistryError as exc:
        print(json.dumps({"subject": subject, "revision": a.revision, "registered": False,
                          "status": exc.http_status_code, "error": exc.error_message}))  # fmt: skip
        return 1
    print(json.dumps({"subject": subject, "revision": a.revision, "schema_id": schema_id}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
