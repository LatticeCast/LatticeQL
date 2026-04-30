"""CLI implementation for the lqlc compiler."""

from __future__ import annotations

import argparse
import sys

from .error import LatticeQLError
from .schema import Schema, load_schema


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lqlc",
        description="Compile a LatticeQL query to PostgreSQL SQL.",
    )
    p.add_argument(
        "lql_file", nargs="?", metavar="FILE", help="LatticeQL source file (default: stdin)"
    )

    schema_group = p.add_mutually_exclusive_group(required=True)
    schema_group.add_argument("--schema", metavar="FILE", help="Path to schema JSON file")
    schema_group.add_argument("--schema-json", metavar="JSON", help="Inline schema JSON string")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Load LQL source
    if args.lql_file:
        try:
            lql = open(args.lql_file, encoding="utf-8").read()
        except OSError as exc:
            print(f"lqlc: error reading {args.lql_file!r}: {exc}", file=sys.stderr)
            return 1
    else:
        lql = sys.stdin.read()

    # Load schema
    try:
        if args.schema:
            schema: Schema = load_schema(args.schema)
        else:
            schema = load_schema(args.schema_json)
    except LatticeQLError as exc:
        print(f"lqlc: schema error: {exc}", file=sys.stderr)
        return 1

    # Compile
    try:
        from . import compile as lql_compile

        sql = lql_compile(lql, schema)
    except LatticeQLError as exc:
        print(f"lqlc: {exc}", file=sys.stderr)
        return 1

    print(sql)
    return 0
