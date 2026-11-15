"""Gate B traffic: one batch of the randomized corpus, published to the real topics."""

from __future__ import annotations

SEED, ROBOTS, TICKS, RUNS = 23, 20, 300, 5
PARTITIONS = {
    "telemetry.raw.v1": 6,
    "fleet.robot-events.v1": 3,
    "fleet.mission-events.v1": 3,
    "fleetforge.fleet.firmware_versions": 1,
}  # same as infrastructure/kafka


def publish(batch: int) -> int:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from fleetforge.config import get_settings
    from fleetforge.kafka.schemas import register, registry_client, subject_for
    from fleetforge.lakehouse import fixtures as fx
    from fleetforge.lakehouse.silver import TELEMETRY_TOPIC

    s = get_settings()
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    missing = set(PARTITIONS) - set(admin.list_topics(timeout=30).topics)
    compacted = {"cleanup.policy": "compact"}
    new = [
        NewTopic(t, PARTITIONS[t], 1, config=compacted if t.startswith("fleetforge.") else {})
        for t in missing
    ]
    for f in (admin.create_topics(new) if new else {}).values():
        f.result(30)
    client = registry_client(s.schema_registry_url)
    ids = {rev: register(client, subject_for(TELEMETRY_TOPIC), rev) for rev in (1, 2)}
    remap = {fx.SCHEMA_V1: ids[1], fx.SCHEMA_V2: ids[2], fx.UNKNOWN_THEN_FOUND: ids[1]}
    corpus = fx.random_corpus(SEED, robots=ROBOTS, ticks=TICKS, runs=RUNS)
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True})
    for r in corpus.batches[batch]:
        value = r.value
        if value and len(value) >= 5 and value[0] == 0:
            sid = int.from_bytes(value[1:5], "big")
            value = b"\x00" + remap[sid].to_bytes(4, "big") + value[5:]
        p.produce(
            r.topic,
            key=r.key,
            value=value,
            partition=r.partition % PARTITIONS[r.topic],
            headers=list(r.headers),
        )
        p.poll(0)
    assert p.flush(60) == 0
    return len(corpus.batches[batch])
