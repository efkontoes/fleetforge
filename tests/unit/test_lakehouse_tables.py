"""Table specifications: internal consistency, DDL, and the generated document."""

from __future__ import annotations

import re
from pathlib import Path

from fleetforge.lakehouse.tables import ALL_TABLES, CATALOG, render_markdown, type_names

ROOT = Path(__file__).resolve().parents[2]


def test_table_specs_are_internally_consistent():
    names = set()
    for t in ALL_TABLES:
        assert t.qualified not in names
        names.add(t.qualified)
        cols = t.column_names
        assert len(cols) == len(set(cols)), t.qualified
        for c in t.identity:
            assert not t.column(c).nullable, f"{t.qualified}.{c}: identity must be NOT NULL"
        for expr in (*t.partitioning, *t.sort_order):
            col = re.sub(r"^\w+\((?:\d+, )?(\w+)\)$", r"\1", expr)
            assert col in cols, f"{t.qualified}: {expr}"
        for c in t.columns:
            type_names(c.type)  # every type has documented Iceberg/Trino equivalents
            assert "'" not in c.doc, f"{t.qualified}.{c.name}: quote breaks the DDL"
        assert "'" not in t.doc


def test_ddl_declares_constraints_partitioning_and_sort_order():
    from fleetforge.lakehouse.tables import SILVER_TELEMETRY_EVENTS as t

    ns, create, order = t.create_statements()
    assert ns == f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver"
    assert f"CREATE TABLE IF NOT EXISTS {CATALOG}.silver.telemetry_events" in create
    assert "event_id STRING NOT NULL" in create and "heading_rad DOUBLE COMMENT" in create
    assert "PARTITIONED BY (days(observed_at), bucket(16, robot_id))" in create
    assert "'format-version' = '2'" in create
    assert order.endswith("WRITE ORDERED BY robot_id, boot_number, sequence_number")


def test_committed_schema_document_is_generated_from_the_specs():
    doc = (ROOT / "docs/architecture/milestone-3-tables.md").read_text()
    assert doc == render_markdown(), "run: python scripts/render_lakehouse_tables.py"
