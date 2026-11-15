"""Gate B's fresh schema must be new to the registry yet read revision 2 records unchanged."""

import io
import json

import fastavro

from fleetforge.kafka.schemas import schema_str
from fleetforge.lakehouse import fixtures as fx
from scripts.gate_b_publish import ROBOTS, RUNS, SEED, TICKS, fresh_v2


def test_fresh_schema_is_new_content():
    assert fresh_v2("1") != schema_str(2)
    assert fresh_v2("1") != fresh_v2("2")


def test_fresh_schema_reads_revision_2_records_unchanged():
    corpus = fx.random_corpus(SEED, robots=ROBOTS, ticks=TICKS, runs=RUNS)
    bodies = [
        r.value[5:]
        for r in corpus.batches[4]
        if r.value and r.value[0] == 0 and int.from_bytes(r.value[1:5], "big") == fx.SCHEMA_V2
    ]
    assert bodies  # the batch Gate B publishes under the fresh id
    v2 = fastavro.parse_schema(json.loads(schema_str(2)))
    fresh = fastavro.parse_schema(json.loads(fresh_v2("1")))
    for body in bodies:
        expected = fastavro.schemaless_reader(io.BytesIO(body), v2, None)
        assert fastavro.schemaless_reader(io.BytesIO(body), fresh, None) == expected
