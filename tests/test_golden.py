"""Golden file tests: compile each _site/examples/*.lql and compare to *.sql.

Set LQLC_REGEN=1 to regenerate all .sql files from actual compiler output.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import lattice_ql

EXAMPLES_DIR = Path(__file__).parent.parent / "_site" / "examples"
SCHEMA_PATH = EXAMPLES_DIR / "_schema.json"

# These goldens need features not yet implemented in v0.1 scaffold.
# Remove entries here as each feature lands.
XFAIL = {
    "060_having.lql",  # HAVING via filter-after-aggregate (resolver context)
    "090_match_with_column.lql",  # with_column stage
    "110_sprint_health_completion.lql",  # variable binding, $params
}


def _schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def _lql_cases() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.lql"))


@pytest.mark.parametrize("lql_path", _lql_cases(), ids=lambda p: p.name)
def test_golden(lql_path: Path) -> None:
    if lql_path.name in XFAIL:
        pytest.xfail(f"{lql_path.name}: not yet implemented")

    lql = lql_path.read_text()
    sql_path = lql_path.with_suffix(".sql")
    schema = _schema()

    actual = lattice_ql.compile(lql, schema)

    if os.environ.get("LQLC_REGEN"):
        sql_path.write_text(actual)
        pytest.skip(f"regenerated {sql_path.name}")
        return

    expected = sql_path.read_text() if sql_path.exists() else ""
    assert actual.strip() == expected.strip(), (
        f"\n--- expected ({sql_path.name}) ---\n{expected}\n--- actual ---\n{actual}"
    )
