# Changelog

All notable changes to LatticeQL (Python) are documented here.

---

### v0.6.1 — 2026-10-24 — row lists can sort by a row timestamp

- The source subquery now selects `created_at` and `updated_at` alongside
  `row_id` and `row_data`, so `sort_desc("updated_at")` resolves instead of
  failing with `column "updated_at" does not exist`. The outer projection still
  exposes only `row_id` and `row_data`.

### v0.6 — 2026-10-24 — workspace-scoped table-name resolution

- Breaking: schema no longer accepts `table_id`; it contains table names and
  columns only.
- `compile()` returns `CompiledQuery(sql, params)`. The table name comes from
  the query text and is inlined as a string literal, so a compiled query cannot
  be retargeted by its caller; the only execution parameter is
  `:workspace_id`, which resolves `rows.table_id` through `tables` under
  row-level security. `params` is empty — the compiler binds nothing of its
  own — and free `$param` references still emit positional `$1..$n` for the
  host to bind.
- Fixed: `filter` after `aggregate` (HAVING) referenced the output alias.
  PostgreSQL resolves neither SELECT aliases nor aggregate names in HAVING, so
  `g.measure` / `g.dim_0` now expand to the expression they stand for
  (`COUNT(*)`, the grouping expression). GIN containment in HAVING (string
  equality on a column) raises a `CodegenError` instead of emitting a
  row-level predicate. Non-aggregate row lists project only `row_id` and
  `row_data`.

### v0.5 — 2026-10-23 — rows are addressed by the table identity alone

- Breaking: emitted SQL no longer carries `AND workspace_id = $1`. Rows have no
  workspace of their own; the table identity addresses them and the caller's
  workspace authorises it through row-level security.
- Parameters therefore start at `$1` instead of `$2`; the CLI no longer has an
  implicit first placeholder to fill.
- `_site/examples` goldens updated to the workspace-free predicates.

---

### v0.4 — 2026-10-23 — tables are addressed by name and identified by a number

- Breaking: `schema["<table_name>"]["table_id"]` is the table's BIGINT identity, not a
  slug string. `Schema.from_dict` normalises it with `int()` and raises `SchemaError`
  when it is not numeric, so a stale slug fails loudly instead of compiling.
- `TableMeta.table_id` is now `int`; queries still name tables (`table("Tasks")`) and the
  compiler inlines the identity from the schema dict it was handed — no lookup query.
- `codegen` emits the identity as a bare numeric literal, so the BIGINT primary key is
  used and nothing from the schema can be injected into the SQL text.
- Golden examples and the `_site/examples` schema moved from slug ids to numbers.

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
