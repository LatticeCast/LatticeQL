"""Tests for schema loader: dict, JSON string, file path, Path object, and CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from lattice_ql.error import SchemaError
from lattice_ql.schema import Schema, load_schema

EXAMPLES_DIR = Path(__file__).parent.parent / "_site" / "examples"
SCHEMA_PATH = EXAMPLES_DIR / "_schema.json"
SCHEMA_DATA: dict = json.loads(SCHEMA_PATH.read_text())


def test_load_schema_from_dict() -> None:
    schema = load_schema(SCHEMA_DATA)
    assert isinstance(schema, Schema)
    col = schema.lookup_column("Tasks", "status")
    assert col is not None
    assert col.id == "col-status"


def test_load_schema_from_json_string() -> None:
    schema = load_schema(SCHEMA_PATH.read_text())
    col = schema.lookup_column("Tasks", "priority")
    assert col is not None
    assert col.id == "col-priority"


def test_load_schema_from_file_path_str() -> None:
    schema = load_schema(str(SCHEMA_PATH))
    col = schema.lookup_column("Deals", "amount")
    assert col is not None
    assert col.id == "col-deal-amount"


def test_load_schema_from_path_object() -> None:
    schema = load_schema(SCHEMA_PATH)
    col = schema.lookup_column("Tasks", "estimate")
    assert col is not None
    assert col.id == "col-estimate"


def test_load_schema_missing_file_raises() -> None:
    with pytest.raises(SchemaError, match="Cannot read schema file"):
        load_schema("/nonexistent/path/schema.json")


def test_load_schema_invalid_json_raises() -> None:
    with pytest.raises(SchemaError, match="Invalid JSON"):
        load_schema("{not valid json")


def test_cli_schema_file(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", str(lql_file), "--schema", str(SCHEMA_PATH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "COUNT(*)" in result.stdout
    assert "Tasks" in result.stdout


def test_cli_schema_json(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lattice_ql",
            str(lql_file),
            "--schema-json",
            SCHEMA_PATH.read_text(),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "COUNT(*)" in result.stdout


def test_cli_stdin(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", "--schema", str(SCHEMA_PATH)],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "COUNT(*)" in result.stdout


def test_cli_missing_schema_arg() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql"],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0


def test_cli_bad_schema_file() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", "--schema", "/nonexistent/schema.json"],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "schema error" in result.stderr
