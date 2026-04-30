"""Tests for lattice_ql.cli.main() — covers success, error, and stdin paths."""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import pytest

from lattice_ql.cli import main

EXAMPLES_DIR = Path(__file__).parent.parent / "_site" / "examples"
SCHEMA_PATH = EXAMPLES_DIR / "_schema.json"
SCHEMA_JSON = SCHEMA_PATH.read_text()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema_args_file() -> list[str]:
    return ["--schema", str(SCHEMA_PATH)]


def _schema_args_json() -> list[str]:
    return ["--schema-json", SCHEMA_JSON]


# ---------------------------------------------------------------------------
# Unit tests (call main() directly, monkeypatching stdin as needed)
# ---------------------------------------------------------------------------


def test_main_returns_0_with_file(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    rc = main([str(lql_file)] + _schema_args_file())
    assert rc == 0


def test_main_returns_0_with_schema_json(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    rc = main([str(lql_file)] + _schema_args_json())
    assert rc == 0


def test_main_returns_1_on_missing_lql_file() -> None:
    rc = main(["/nonexistent/query.lql"] + _schema_args_file())
    assert rc == 1


def test_main_returns_1_on_bad_schema_file(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    rc = main([str(lql_file), "--schema", "/nonexistent/schema.json"])
    assert rc == 1


def test_main_returns_1_on_invalid_schema_json(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    rc = main([str(lql_file), "--schema-json", "{bad json"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "schema error" in captured.err


def test_main_returns_1_on_compile_error(tmp_path: Path) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text(
        'table("Tasks") | filter((r) -> { r.nonexistent_col == "x" }) | aggregate(count())\n'
    )

    rc = main([str(lql_file)] + _schema_args_file())
    assert rc == 1


def test_main_reads_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO('table("Tasks") | aggregate(count())\n'))

    rc = main(_schema_args_file())
    assert rc == 0


def test_main_output_contains_sql(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text('table("Tasks") | aggregate(count())\n')

    rc = main([str(lql_file)] + _schema_args_file())
    assert rc == 0
    captured = capsys.readouterr()
    assert "COUNT(*)" in captured.out
    assert "Tasks" in captured.out
    assert captured.out.strip().endswith(";")


def test_main_group_by_output(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    lql_file = tmp_path / "q.lql"
    lql_file.write_text(
        'table("Tasks")\n'
        '  | filter((r) -> { r.status != "merged" })\n'
        "  | group_by((r) -> { r.priority })\n"
        "  | aggregate(count())\n"
    )

    rc = main([str(lql_file)] + _schema_args_file())
    assert rc == 0
    out = capsys.readouterr().out
    assert "GROUP BY" in out
    assert "COUNT(*)" in out


# ---------------------------------------------------------------------------
# Subprocess tests — exercises the installed entry point path
# ---------------------------------------------------------------------------


def test_subprocess_file_and_schema_file(tmp_path: Path) -> None:
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


def test_subprocess_stdin_and_schema_json() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", "--schema-json", SCHEMA_JSON],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "COUNT(*)" in result.stdout


def test_subprocess_no_schema_arg_exits_nonzero() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql"],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0


def test_subprocess_bad_schema_file_exits_1() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", "--schema", "/no/such/file.json"],
        input='table("Tasks") | aggregate(count())\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "schema error" in result.stderr


def test_subprocess_missing_lql_file_exits_1(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lattice_ql", "/no/such/query.lql", "--schema", str(SCHEMA_PATH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "error reading" in result.stderr
