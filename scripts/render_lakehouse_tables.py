"""Regenerate docs/architecture/milestone-3-tables.md from the table specifications."""

from pathlib import Path

from fleetforge.lakehouse.tables import render_markdown

out = Path(__file__).resolve().parents[1] / "docs/architecture/milestone-3-tables.md"
out.write_text(render_markdown())
print(f"wrote {out}")
