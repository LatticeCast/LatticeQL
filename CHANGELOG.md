# Changelog

All notable changes to LatticeQL (Python) are documented here.

---

### v0.1 — 2026-04-30 — Python text-to-text compiler MVP

- Added `pyproject.toml` — `lattice-ql` package, editable install (`pip install -e .`), `lqlc` entry point
- Added `src/lattice_ql/` package layout with `__init__.py` exposing `compile(lql, schema) -> str`
- Added `lattice_ql.lexer` — regex-driven single-pass tokenizer; supports strings, numbers, booleans, null, operators (`|`, `->`, `:=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, `!`), `@`, `$param`
- Added `lattice_ql.ast` — typed dataclass AST nodes: `Program`, `LetBinding`, `Query`, all stage types, all expression types, `GinContainment`
- Added `lattice_ql.parser` — hand-written recursive-descent parser; parses full pipeline DSL including `match` expressions, `@{...}` record literals, `@[...]` list literals, pipe-form built-ins (`bucket`)
- Added `lattice_ql.schema` — `Schema` / `ColumnMeta` dataclasses; loads from JSON file path, JSON string, or `dict`; `UNIQUE(workspace, table_name)` semantics enforced
- Added `lattice_ql.resolver` — maps human column names to `col_id` UUIDs in `row_data`; rejects unknown column names at compile time
- Added `lattice_ql.sema` — semantic analysis pass: GIN flattening (consecutive `field == "str"` AND-chains collapse into a single `@>` containment check), `bucket()` requires `date`-typed column, numeric aggregates require `number`-typed column, `having()` requires a prior `aggregate()` in the pipeline
- Added `lattice_ql.codegen` — SQL emitter: `SELECT … FROM rows WHERE table_id = (…) [AND …] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]`; `$1` = `workspace_id`, subsequent `$N` for `$param` references in order of appearance
- Supported pipeline primitives: `table`, `filter`, `with_column` (`match` value and guard forms), `group_by` (positional and named), `aggregate` (single and named-map form, `*_if` conditional family, `percentile`, `median`, `stddev`, `variance`), `sort_asc` / `sort_desc`, `limit`, post-aggregate `filter` (compiles to HAVING), `having`
- Supported expressions: field access (`r.col`), numeric/string/bool/null literals, `$param` references, binary operators (`==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`), unary `!`, `in @[...]` list membership, `match` / CASE–WHEN, `bucket(unit)` pipe (UTC date_trunc), virtual columns via `with_column`
- Added `:=` named bases — compile-time inline expansion; `VarRefStage` spliced into the referencing pipeline
- Added `lattice_ql.cli` — `lqlc` binary; reads `.lql` from file or stdin; `--schema FILE` and `--schema-json JSON` (mutually exclusive); exits 0 on success, 1 on any compiler error
- Added golden tests (`tests/test_golden.py`) against all 11 `_site/examples/*.lql` / `*.sql` fixture pairs
- Added unit tests for schema loader, resolver, sema, codegen, and CLI
- v0.2 deferred: `join`, `lookup`, `lookup_recursive`, `with_window`, `unnest`, set ops (`union` / `intersect` / `except`), `distinct`, `has` / `not_has`, `project`, `--emit-params` sidecar, `--workspace` literal substitution, WASM build
