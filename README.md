# LatticeQL

> LatticeQL is the query language for LatticeCast dashboards. Syntax is inspired by Homun-Lang (pipe, lambda, immutable bind), but LatticeQL is its own language: it compiles to PostgreSQL JSONB SQL, has its own primitives (`table`, `lookup`, `join`, `bucket`, `with_column`, `with_window`, `aggregate`, ...), and is tuned for hitting GIN indexes by default.
>
> One file = one panel. Pipe stages read top-to-bottom like a recipe.

---

## Python package — quick start

**Install (editable):**

```bash
pip install -e ".[dev]"
```

**Python API:**

```python
from lattice_ql import compile

schema = {
    "Tasks": {
        "table_id": "tbl-tasks",
        "columns": {
            "status":   {"id": "col-status",   "type": "select"},
            "priority": {"id": "col-priority", "type": "select"}
        }
    }
}

sql = compile('table("Tasks") | aggregate(count())', schema)
print(sql)
```

**CLI (`lqlc`):**

```bash
# file + schema file
lqlc query.lql --schema schema.json

# stdin + inline JSON schema
echo 'table("Tasks") | aggregate(count())' | lqlc --schema-json '{"Tasks": {...}}'
```

The compiler pipeline is: **lexer → parser → resolver → sema → codegen**.
`$1` is always `workspace_id`; additional `$param` values follow in order of first appearance.

See `CHANGELOG.md` for what is implemented in v0.1 and what is deferred.

---

## 0. Why this exists

LatticeCast core is Airtable-like JSONB engine (Postgres GIN index on jsonb) as core, and extend to project management system by just adding timeline view, kanban view, table view.

Further, by just add dashboard view, it become CRM system. However, the layout is able to define any query from tables in workspace and edit layout. You can build dashboards (bar / pie / gauge / stat / line) on top of it.



---

## 1. Core conventions

### 1.1 Table addressing — by name, never by UUID

LatticeCast stores all rows in a single `rows` table, with `table_id` UUID FK distinguishing logical tables. **LatticeQL never exposes the UUID.**

```latticeql
table("Tasks")
```

Compiles to:

```sql
FROM rows
WHERE table_id = (
  SELECT table_id FROM tables
  WHERE table_name = 'Tasks' AND workspace_id = $workspace_id
)
```

The `tables` catalog has `UNIQUE(workspace_id, table_name)`, so the subquery returns exactly one row. PG folds it efficiently. Authors write `table("Tasks")`; readers of generated SQL see `table_name = 'Tasks'`. UUIDs stay internal.

### 1.2 Column references — by name in source, by id in row_data

Inside a row, columns are stored as JSONB keyed by `column_id` (UUID, stable across renames). LatticeQL source uses human names (`r.status`); the compiler resolves to `row_data->>'<status_col_id>'` at compile time by reading `tables.columns`.

For doc readability, examples below write `row_data->>'<status>'` to mean "the JSONB key for the column named `status`".

### 1.3 Workspace context

Every LatticeQL query runs inside a workspace. The compiler injects `workspace_id = $1` wherever it resolves a table_name. Cross-workspace queries are rejected at compile time.

### 1.4 Time is UTC — always

**All timestamps in LatticeCast are stored, compared, bucketed, and returned as UTC.**

- Column type `date` is backed by `timestamptz` and **always normalized to UTC** on write.
- All literal date strings in LatticeQL (`"2026-06-01"`, `"2026-06-01T00:00:00Z"`) are interpreted as **UTC**.
- `bucket("day"|"week"|"month"|...)` runs `date_trunc(unit, x AT TIME ZONE 'UTC')` — boundaries are UTC midnights, UTC weeks (Mon 00:00 UTC), UTC months.
- Comparison literals (`r.due_date < "2026-06-01"`) compile to `timestamptz < '2026-06-01T00:00:00Z'`.
- The frontend handles all user-timezone formatting; the wire format is always UTC ISO-8601.

This eliminates an entire class of bug (DST, ambiguous local-time boundaries) at the cost of one rule: **never write a local-timezone literal in LatticeQL.** If a panel really needs "rows in Asia/Taipei calendar day", the frontend converts the user's local boundary to a UTC instant and passes it as a parameter (§23).

→ **Throughout this doc, every date example is UTC.** No `at_timezone` primitive exists; LatticeQL has no concept of local time.

---

## 2. Your first query

Goal: **count rows in `Tasks`.**

```latticeql
table("Tasks") | aggregate(count())
```

Compiles to:

```sql
SELECT COUNT(*) AS measure
FROM rows
WHERE table_id = (SELECT table_id FROM tables
                  WHERE table_name = 'Tasks' AND workspace_id = $1);
```

Returns:

```json
{ "measure": 47 }
```

Two ideas to remember:

1. **`table("Tasks")` is the source.** Resolved by name, scoped to the current workspace.
2. **`| aggregate(count())` is the terminus.** Most queries end with `aggregate`; row-list queries don't (see §24).

---

## 3. Filtering

Goal: **count rows where status is "todo".**

```latticeql
table("Tasks")
  | filter((r) -> { r.status == "todo" })
  | aggregate(count())
```

Compiles to:

```sql
SELECT COUNT(*) AS measure
FROM rows
WHERE table_id = (SELECT table_id FROM tables
                  WHERE table_name = 'Tasks' AND workspace_id = $1)
  AND row_data @> '{"<status>": "todo"}'::jsonb;
```

Three ideas:

1. **`(r) -> { ... }` is a lambda; `r` is a row.** Standard Homun-style lambda.
2. **`r.status` uses the human column name.** Compiler maps `status` → its `column_id` in row_data.
3. **`==` compiles to `@>` containment**, which uses the GIN index on row_data. This is the path to fast filtering.

---

## 4. Grouping

Goal: **count tasks per priority** (pie chart / bar chart).

```latticeql
table("Tasks")
  | filter((r) -> { r.status != "merged" })
  | group_by((r) -> { r.priority })
  | aggregate(count())
```

Compiles to:

```sql
SELECT row_data->>'<priority>' AS dim_0,
       COUNT(*)                AS measure
FROM rows
WHERE table_id = (SELECT table_id FROM tables
                  WHERE table_name = 'Tasks' AND workspace_id = $1)
  AND NOT (row_data @> '{"<status>":"merged"}'::jsonb)
GROUP BY dim_0
ORDER BY measure DESC;
```

Returns:

```json
[
  { "dim_0": "high",     "measure": 12 },
  { "dim_0": "medium",   "measure": 8  },
  { "dim_0": "low",      "measure": 3  },
  { "dim_0": "critical", "measure": 2  }
]
```

Each `| group_by` adds a `dim_N`. Stacked bars use two `group_by` stages.

### Named dimensions (optional)

For panels that read `dim_0`/`dim_1` by position, the default works. For complex dashboards prefer named dims:

```latticeql
| group_by(@{ "team": (r) -> { r.team }, "month": (r) -> { r.created_at | bucket("month") } })
```

Output rows then have keys `team`, `month` instead of `dim_0`, `dim_1`. Mixing positional and named in the same pipeline is rejected at compile time.

---

## 5. Measures — aggregates

Goal: **per priority, get count, average estimate, total estimate, median, p95.**

```latticeql
table("Tasks")
  | filter((r) -> { r.type == "task" })
  | group_by((r) -> { r.priority })
  | aggregate(@{
      "count":     count(),
      "avg_pts":   avg(r.estimate),
      "total_pts": sum(r.estimate),
      "median":    median(r.estimate),
      "p95":       percentile(r.estimate, 0.95)
    })
```

Compiles to:

```sql
SELECT row_data->>'<priority>'                                            AS dim_0,
       COUNT(*)                                                           AS count,
       AVG((row_data->>'<estimate>')::numeric)                            AS avg_pts,
       SUM((row_data->>'<estimate>')::numeric)                            AS total_pts,
       PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY (...)::numeric)       AS median,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (...)::numeric)       AS p95
FROM rows
WHERE table_id = (SELECT table_id FROM tables
                  WHERE table_name = 'Tasks' AND workspace_id = $1)
  AND row_data @> '{"<type>":"task"}'::jsonb
GROUP BY dim_0;
```

`aggregate` accepts two forms:

| Form | Use |
|---|---|
| `aggregate(count())` | Single measure, output column named `measure` |
| `aggregate(@{"a": expr1, "b": expr2})` | Multiple measures, custom names |

### Available aggregates

| LatticeQL | SQL | Notes |
|---|---|---|
| `count()` | `COUNT(*)` | |
| `count_distinct(r.x)` | `COUNT(DISTINCT row_data->>'<x>')` | |
| `sum(r.x)` | `SUM((row_data->>'<x>')::numeric)` | |
| `avg(r.x)` | `AVG((row_data->>'<x>')::numeric)` | |
| `min(r.x)` / `max(r.x)` | `MIN(...)` / `MAX(...)` | |
| `median(r.x)` | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ...)` | |
| `percentile(r.x, p)` | `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY ...)` | `p` is `0.0`–`1.0` literal |
| `stddev(r.x)` | `STDDEV_POP(...)` | population stddev |
| `variance(r.x)` | `VAR_POP(...)` | |

### Conditional aggregates — `*_if` family

Filter at the measure level instead of a separate `filter` stage. Critical for CRM ("won pipeline value") where one query mixes won / lost / open measures:

```latticeql
table("Deals")
  | group_by((r) -> { r.owner })
  | aggregate(@{
      "won_count":  count_if((r) -> { r.status == "won" }),
      "won_value":  sum_if((r) -> { r.status == "won" }, r.amount),
      "open_value": sum_if((r) -> { r.status == "open" }, r.amount),
      "win_rate":   avg_if((r) -> { r.status in @["won", "lost"] },
                            match r.status { "won" -> 1; _ -> 0 })
    })
```

Compiles to:

```sql
SELECT row_data->>'<owner>'                                                          AS dim_0,
       COUNT(*) FILTER (WHERE row_data @> '{"<status>":"won"}')                      AS won_count,
       SUM((row_data->>'<amount>')::numeric) FILTER (WHERE row_data @> '{"<status>":"won"}')   AS won_value,
       SUM((row_data->>'<amount>')::numeric) FILTER (WHERE row_data @> '{"<status>":"open"}')  AS open_value,
       AVG(CASE WHEN row_data @> '{"<status>":"won"}'  THEN 1
                WHEN row_data @> '{"<status>":"lost"}' THEN 0 END)
         FILTER (WHERE row_data @> '{"<status>":"won"}'
                    OR row_data @> '{"<status>":"lost"}')                             AS win_rate
FROM rows
WHERE table_id = (...)
GROUP BY dim_0;
```

The compiler uses `FILTER (WHERE ...)` so the predicate still hits the GIN index (it's an ordinary boolean clause, just scoped to the aggregate). Available: `count_if`, `sum_if`, `avg_if`, `min_if`, `max_if`, `count_distinct_if`.

---

## 6. Sort, limit, offset

Goal: **top 10 busiest assignees by total workload.**

```latticeql
table("Tasks")
  | filter((r) -> { r.status in @["todo", "in_progress", "testing"] })
  | group_by((r) -> { r.assignee })
  | aggregate(@{ "tickets": count(), "workload": sum(r.estimate) })
  | sort_desc("workload")
  | limit(10)
```

```sql
SELECT row_data->>'<assignee>' AS dim_0,
       COUNT(*)                                AS tickets,
       SUM((row_data->>'<estimate>')::numeric) AS workload
FROM rows
WHERE table_id = (...)
  AND ( row_data @> '{"<status>":"todo"}'::jsonb
     OR row_data @> '{"<status>":"in_progress"}'::jsonb
     OR row_data @> '{"<status>":"testing"}'::jsonb )
GROUP BY dim_0
ORDER BY workload DESC
LIMIT 10;
```

`sort_desc` / `sort_asc` take a string — either a measure name (`"workload"`) or a dimension name (`"dim_0"`).

### Pagination — `offset`

For table views and "page N" navigation:

```latticeql
| sort_asc("created_at")
| limit(20)
| offset(40)        // page 3 (0-indexed) of 20
```

```sql
ORDER BY (row_data->>'<created_at>')::timestamptz ASC
LIMIT 20 OFFSET 40
```

For deep pagination (>10k rows) prefer keyset pagination — pass `r.row_number > $cursor` as a filter parameter (§23) and drop offset entirely.

---

## 7. One query, many charts

LatticeQL produces **data**, not chart types. All charts consume the same shape:

```
[ { dim_0, dim_1, ..., measure_1, measure_2, ... } ]
```

| Chart | Dims | Measures | Source pattern |
|---|---|---|---|
| `stat` (single number) | 0 | 1 | No `group_by`, single `aggregate` |
| `gauge` | 0 | 1 | Same as stat, panel adds thresholds |
| `pie` | 1 | 1 | One `group_by` + count/sum |
| `bar` | 1 | 1+ | Same as pie, multi-measure ok |
| `stacked bar` | 2 | 1 | Two `group_by` |
| `line` | 1 (date_bucket) | 1+ | `bucket()` over a date column |
| `funnel` | 1 (stage) | 1+ | One `group_by` + ordered stages |
| `heatmap` | 2 | 1 | Two `group_by` (often dim_0=hour, dim_1=day) |
| `table` | N | M | Anything |

→ The same LatticeQL feeds different chart types. Chart type lives in panel config.

---

## 8. WHERE vs HAVING — filter timing

Goal: **assignees with more than 10 unmerged tickets.**

```latticeql
table("Tasks")
  | filter((r) -> { r.status != "merged" })   // before aggregate
  | group_by((r) -> { r.assignee })
  | aggregate(count())
  | filter((g) -> { g.measure > 10 })         // after aggregate
  | sort_desc("measure")
  | limit(5)
```

```sql
SELECT row_data->>'<assignee>' AS dim_0, COUNT(*) AS measure
FROM rows
WHERE table_id = (...)
  AND NOT (row_data @> '{"<status>":"merged"}'::jsonb)
GROUP BY dim_0
HAVING COUNT(*) > 10
ORDER BY measure DESC
LIMIT 5;
```

| Position | Compiles to | Lambda parameter convention |
|---|---|---|
| `filter` BEFORE `aggregate` | `WHERE` | `r` (one row) |
| `filter` AFTER `aggregate` | `HAVING` | `g` (one group; access `g.measure`, `g.dim_0`, custom measure names) |

---

## 9. Time bucketing — UTC by default

Goal: **monthly ticket creation count** (line chart / burn-up).

```latticeql
table("Tasks")
  | group_by((r) -> { r.created_at | bucket("month") })
  | aggregate(count())
  | sort_asc("dim_0")
```

```sql
SELECT date_trunc('month', (row_data->>'<created_at>')::timestamptz AT TIME ZONE 'UTC') AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = (...)
GROUP BY dim_0
ORDER BY dim_0 ASC;
```

`bucket()` units: `"hour"`, `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`.

`r.created_at | bucket("month")` and `bucket(r.created_at, "month")` are equivalent. Pipe form reads better in pipelines.

**All buckets are UTC** (per §1.4). A "day" boundary is UTC midnight, not the user's local midnight. The frontend handles user-timezone display.

---

## 10. Sets, membership, ranges

### 10.1 `in` over select column → OR-of-`@>`

```latticeql
filter((r) -> { r.status in @["todo", "in_progress"] })
```

```sql
AND ( row_data @> '{"<status>":"todo"}'::jsonb
   OR row_data @> '{"<status>":"in_progress"}'::jsonb )
```

PG combines these with **Bitmap Index Scan + BitmapOr**, so each branch still uses the GIN index — total cost is `O(matching rows)`, not `O(table size)`. Slower than a single flat `@>` (§12), but still fully indexed. Don't avoid `or` to chase performance; only flatten when it's natural.

### 10.2 `in` over tags array → array containment

```latticeql
filter((r) -> { "urgent" in r.tags })
```

```sql
AND row_data @> '{"<tags>":["urgent"]}'::jsonb
```

JSONB array containment uses GIN.

### 10.3 Multi-tag (all required)

```latticeql
filter((r) -> { "urgent" in r.tags and "frontend" in r.tags })
```

```sql
AND row_data @> '{"<tags>":["urgent","frontend"]}'::jsonb
```

Both tags merged into one `@>` — single GIN lookup.

### 10.4 Range filters (numbers and UTC dates)

```latticeql
filter((r) -> { r.estimate > 5 and r.due_date < "2026-06-01" })
```

```sql
AND (row_data->>'<estimate>')::numeric > 5
AND (row_data->>'<due_date>')::timestamptz < '2026-06-01T00:00:00Z'::timestamptz
```

Both sides of a date comparison are UTC (§1.4). The string `"2026-06-01"` expands to `'2026-06-01T00:00:00Z'`. These hit B-tree expression indexes (auto-built by LatticeCast on number / date columns).

### 10.5 Not-null

```latticeql
filter((r) -> { r.assignee != none })
```

```sql
AND row_data ? '<assignee>'
```

`?` is JSONB key-existence — GIN-friendly.

---

## 11. Expressions — arithmetic and string ops

Inside any lambda (`filter`, `with_column`, `aggregate`, `group_by`), you can compose:

### 11.1 Arithmetic

```latticeql
with_column("weighted", (r) -> { r.amount * r.probability / 100 })
with_column("net",      (r) -> { r.revenue - r.cost })
```

Operators: `+ - * / %`. All operands cast to `numeric` automatically when the column is a number; mixing string and number is a compile error.

### 11.2 String operations

| LatticeQL | SQL | Notes |
|---|---|---|
| `r.x \| contains("foo")` | `position('foo' in (row_data->>'<x>')) > 0` | case-sensitive substring |
| `r.x \| icontains("foo")` | `(row_data->>'<x>') ILIKE '%foo%'` | case-insensitive |
| `r.x \| starts_with("PRE-")` | `(row_data->>'<x>') LIKE 'PRE-%'` | hits B-tree if prefix index exists |
| `r.x \| ends_with(".pdf")` | `(row_data->>'<x>') LIKE '%.pdf'` | full scan |
| `r.x \| matches("^TASK-\\d+$")` | `(row_data->>'<x>') ~ '^TASK-\d+$'` | POSIX regex |
| `r.x \| lower` / `\| upper` | `lower(...)` / `upper(...)` | normalize for comparison |
| `r.x \| length` | `char_length(...)` | char count |

For full-text search across many text columns prefer the dedicated `text_search()` primitive in §22 (uses `tsvector`).

### 11.3 No string interpolation

`"User: ${r.name}"` is **rejected at compile time** — it's a SQL injection vector. Use a virtual column with concat:

```latticeql
with_column("label", (r) -> { r.first_name | concat(" ", r.last_name) })
```

---

## 12. GIN flattening — the key optimization

Consecutive `==` joined by `and` **collapse into a single `@>` containment check**:

```latticeql
filter((r) -> {
  r.status   == "in_progress" and
  r.priority == "high" and
  r.assignee == "claude"
})
```

Compiles to:

```sql
AND row_data @> '{
  "<status>":   "in_progress",
  "<priority>": "high",
  "<assignee>": "claude"
}'::jsonb
```

→ **One GIN lookup answers all three conditions.** Fastest path in JSONB.

### When flattening fails

| Pattern | Behavior |
|---|---|
| All `==` joined by `and` | ✅ Full flatten into single `@>` |
| Any `or` | ⚠️ OR splits, each side flattens internally; PG does Bitmap OR — still indexed, slightly slower |
| Any `!=` | ⚠️ Becomes `NOT @>`, other `==` still flatten |
| Any `<` / `>` | ⚠️ Range uses expression index, other `==` still flatten |
| Any `in @[...]` | ⚠️ Expands to OR-of-`@>` |
| Nested `(... or ...)` | ⚠️ Subexpression flattens internally only |
| String ops (`contains`, regex) | ❌ Sequential scan unless trigram index exists |

→ **To max GIN performance, write filters as a flat `==`-and-`and` chain.** Style controls speed.

---

## 13. Virtual columns — `with_column` + `match`

Goal: **score each issue by priority, then average per team.**

```latticeql
table("Issues")
  | with_column("urgency", (r) -> {
      match r.priority {
        "critical" -> 100
        "high"     -> 50
        "medium"   -> 20
        _          -> 0
      }
    })
  | group_by((r) -> { r.team })
  | aggregate(@{ "avg_urgency": avg(r.urgency) })
```

```sql
SELECT row_data->>'<team>' AS dim_0,
       AVG(
         CASE row_data->>'<priority>'
           WHEN 'critical' THEN 100
           WHEN 'high'     THEN 50
           WHEN 'medium'   THEN 20
           ELSE 0
         END
       ) AS avg_urgency
FROM rows
WHERE table_id = (...)
GROUP BY dim_0;
```

- `with_column(name, lambda)` adds a virtual column. **Not persisted** to row_data — exists only within this query.
- `match` compiles to `CASE WHEN`. Value-to-value mapping or guarded patterns both work.
- Subsequent stages access the virtual column like any other (`r.urgency`).

Guard form:

```latticeql
with_column("size", (r) -> {
  match r.estimate {
    _ if r.estimate > 10 -> "Epic"
    _ if r.estimate > 5  -> "Story"
    _                    -> "Task"
  }
})
```

```sql
CASE
  WHEN (row_data->>'<estimate>')::numeric > 10 THEN 'Epic'
  WHEN (row_data->>'<estimate>')::numeric > 5  THEN 'Story'
  ELSE 'Task'
END
```

---

## 14. Window functions — `with_window`

For running totals, ranks, period-over-period comparisons. Window functions run **after** `aggregate` (or after a row-list source) and add a new column without collapsing rows.

### 14.1 Running total / cumulative

```latticeql
table("Tasks")
  | filter((r) -> { r.status in @["done", "merged"] })
  | group_by((r) -> { r.completed_at | bucket("day") })
  | aggregate(@{ "completed": count() })
  | with_window("cumulative", (w) -> {
      sum(w.completed) order_by w.dim_0
    })
  | sort_asc("dim_0")
```

```sql
SELECT dim_0,
       completed,
       SUM(completed) OVER (ORDER BY dim_0) AS cumulative
FROM (
  SELECT date_trunc('day', (row_data->>'<completed_at>')::timestamptz AT TIME ZONE 'UTC') AS dim_0,
         COUNT(*) AS completed
  FROM rows
  WHERE table_id = (...)
    AND ( row_data @> '{"<status>":"done"}'::jsonb
       OR row_data @> '{"<status>":"merged"}'::jsonb )
  GROUP BY dim_0
) base
ORDER BY dim_0 ASC;
```

### 14.2 Rank within group

```latticeql
table("Deals")
  | group_by(@{ "team": (r) -> { r.team }, "owner": (r) -> { r.owner } })
  | aggregate(@{ "revenue": sum(r.amount) })
  | with_window("rank_in_team", (w) -> {
      rank() partition_by w.team order_by w.revenue desc
    })
  | filter((g) -> { g.rank_in_team <= 3 })   // top 3 owners per team
```

```sql
SELECT team, owner, revenue,
       RANK() OVER (PARTITION BY team ORDER BY revenue DESC) AS rank_in_team
FROM ( ... base aggregate ... ) base
WHERE rank_in_team <= 3
```

### 14.3 Period-over-period (LAG / LEAD)

```latticeql
| with_window("prev_month", (w) -> { lag(w.measure) order_by w.dim_0 })
| with_column("growth", (g) -> {
    match g.prev_month {
      none -> none
      _    -> (g.measure - g.prev_month) / g.prev_month
    }
  })
```

### 14.4 Percent of total

```latticeql
| with_window("pct_of_total", (w) -> {
    w.measure / sum(w.measure) over_all
  })
```

`over_all` is shorthand for "no partition, no order".

### Window function vocabulary

| LatticeQL | SQL |
|---|---|
| `sum(w.x) order_by w.y` | `SUM(x) OVER (ORDER BY y)` |
| `sum(w.x) partition_by w.p order_by w.y` | `SUM(x) OVER (PARTITION BY p ORDER BY y)` |
| `sum(w.x) over_all` | `SUM(x) OVER ()` |
| `rank()` / `dense_rank()` / `row_number()` | as named, with `partition_by` / `order_by` |
| `lag(w.x)` / `lead(w.x)` | `LAG(x)` / `LEAD(x)` over partition+order |
| `lag(w.x, n)` / `lead(w.x, n)` | n-step lag/lead |
| `first_value(w.x)` / `last_value(w.x)` | as named |

`with_window` always produces an outer query — the `aggregate` stage becomes a subquery, the window runs over its result. Multiple `with_window` stages compose without further nesting (they share the same outer level).

---

## 15. Array unnest — per-element aggregation

Tags / multi-select / array columns need explicit explosion to count individual elements:

```latticeql
table("Tasks")
  | unnest(r.tags, as: "tag")
  | group_by((r) -> { r.tag })
  | aggregate(count())
  | sort_desc("measure")
```

```sql
SELECT t.tag AS dim_0, COUNT(*) AS measure
FROM rows r
CROSS JOIN LATERAL jsonb_array_elements_text(r.row_data->'<tags>') AS t(tag)
WHERE r.table_id = (...)
GROUP BY dim_0
ORDER BY measure DESC;
```

After `unnest`, the named alias (`r.tag`) is accessible like any column, alongside the original row's other fields. A row with 3 tags appears 3 times, once per tag — count it accordingly.

For "filter rows that have any tag in [...]" use array containment (§10.2) — `unnest` is only for *aggregating per element*.

---

## 16. Distinct rows

For deduplicating row-list output (table panels):

```latticeql
table("Activities")
  | distinct((r) -> { r.contact_id })   // one row per contact
  | sort_desc("created_at")
  | limit(50)
```

```sql
SELECT DISTINCT ON ((row_data->>'<contact_id>')) row_number, row_data
FROM rows
WHERE table_id = (...)
ORDER BY (row_data->>'<contact_id>'), (row_data->>'<created_at>')::timestamptz DESC
LIMIT 50;
```

`distinct` is rejected before `aggregate` (use `count_distinct` instead) — it only makes sense on row-list pipelines.

---

## 17. Stage reuse — `:=`

Multiple panels in one dashboard often share a base filter. Bind it once with `:=`:

```latticeql
open := table("Tasks")
  | filter((r) -> { not (r.status in @["merged", "done"]) })

p1 := open | group_by((r) -> { r.assignee }) | aggregate(count())
p2 := open | group_by((r) -> { r.priority }) | aggregate(count())
p3 := open | aggregate(count())   // single stat card
```

`:=` is **compile-time inline expansion**, not a runtime CTE. Each panel produces an independent SQL query that can run in parallel — no DB-side coordination, no shared state.

```sql
-- p1
SELECT row_data->>'<assignee>' AS dim_0, COUNT(*) AS measure
FROM rows
WHERE table_id = (...)
  AND NOT ( row_data @> '{"<status>":"merged"}'::jsonb
         OR row_data @> '{"<status>":"done"}'::jsonb )
GROUP BY dim_0;

-- p2, p3 — independent SQL with same base filter, all dispatched in parallel
```

---

## 18. Joining tables — `lookup`, `join`, `lookup_recursive`

Real dashboards cross tables. PM hierarchies have a `parent` column. CRM Deals reference Contacts. Tasks belong to Stories.

### 18.1 `lookup` — left-join enrichment (the common case)

For "show each row + a field denormalized from a referenced row":

```latticeql
table("Deals")
  | with_column("owner_name", (r) -> {
      lookup(table("Contacts"), r.owner_id).full_name
    })
  | group_by((r) -> { r.owner_name })
  | aggregate(@{ "total_value": sum(r.amount) })
```

```sql
WITH _t AS (SELECT table_name, table_id FROM tables WHERE workspace_id = $1)
SELECT c.row_data->>'<full_name>'              AS dim_0,
       SUM((d.row_data->>'<amount>')::numeric) AS total_value
FROM rows d
JOIN _t td ON td.table_id = d.table_id AND td.table_name = 'Deals'
LEFT JOIN _t tc ON tc.table_name = 'Contacts'
LEFT JOIN rows c ON c.table_id = tc.table_id
                AND c.row_number = (d.row_data->>'<owner_id>')::bigint
GROUP BY dim_0;
```

Key points:

- `lookup` compiles to **one `LEFT JOIN` at the SQL level — never N+1**. The name "lookup" describes the user mental model (per-row enrichment), not the execution.
- Joins on `row_number` (the per-table integer PK). PG uses the B-tree PK index — fast.
- Multiple lookups against the same target table merge into one JOIN at compile time.
- If no matching row, the resulting field is `none`.

### 18.2 Self-reference (PM hierarchy, single level)

```latticeql
table("Tasks")
  | filter((r) -> { r.type == "task" })
  | with_column("epic_title", (r) -> { lookup(table("Tasks"), r.parent).title })
  | group_by((r) -> { r.epic_title })
  | aggregate(count())
  | sort_desc("measure")
```

Same table joined to itself by `row_number` reference.

### 18.3 `lookup_recursive` — bounded multi-level hierarchy

For arbitrary-depth trees (org chart, WBS, multi-level epic→story→task) within a configurable depth cap:

```latticeql
table("Tasks")
  | with_column("ancestors", (r) -> {
      lookup_recursive(table("Tasks"),
                       parent_key: r.parent,
                       field: "title",
                       max_depth: 5)
    })
  | filter((r) -> { "Q2 Initiative" in r.ancestors })
  | aggregate(count())
```

`ancestors` is an array of titles from immediate parent up to root (or until `max_depth`). Compiles to a recursive CTE with explicit depth limit — guaranteed to terminate even if the data has a cycle:

```sql
WITH RECURSIVE ancestry AS (
  SELECT row_number, row_data, 0 AS depth,
         ARRAY[]::text[] AS path
  FROM rows
  WHERE table_id = (...) AND ...

  UNION ALL

  SELECT child.row_number, child.row_data, a.depth + 1,
         a.path || (parent.row_data->>'<title>')
  FROM ancestry a
  JOIN rows child  ON child.row_number = a.row_number
  JOIN rows parent ON parent.row_number = (child.row_data->>'<parent>')::bigint
                  AND parent.table_id   = child.table_id
  WHERE a.depth < 5
)
SELECT ... FROM ancestry WHERE depth = 5 OR ... ;
```

`max_depth` is mandatory and capped at workspace config (default 8). Without a cap, recursion is rejected.

### 18.4 `join` — explicit relational join

When you need INNER/LEFT/FULL semantics or a join condition richer than row_number lookup:

```latticeql
deals    := table("Deals")
contacts := table("Contacts")

deals
  | join(contacts, kind: "inner", on: (d, c) -> {
      d.owner_id == c.row_number and c.region == "APAC"
    })
  | filter((r) -> { r.deals.status == "open" })
  | group_by((r) -> { r.contacts.company })
  | aggregate(@{
      "deal_count":  count(),
      "total_value": sum(r.deals.amount)
    })
```

After `join`, row access uses **namespace prefixes**: `r.deals.x`, `r.contacts.y`. Drop the prefix when unambiguous (`r.amount` works if only `deals` has it). Virtual columns added after a join must use a fresh namespace (`with_column("score", ...)` becomes `r.score`, no prefix).

```sql
WITH _t AS (SELECT table_name, table_id FROM tables WHERE workspace_id = $1)
SELECT c.row_data->>'<company>'                AS dim_0,
       COUNT(*)                                AS deal_count,
       SUM((d.row_data->>'<amount>')::numeric) AS total_value
FROM rows d
JOIN _t td ON td.table_id = d.table_id AND td.table_name = 'Deals'
INNER JOIN _t tc ON tc.table_name = 'Contacts'
INNER JOIN rows c ON c.table_id = tc.table_id
                 AND c.row_number = (d.row_data->>'<owner_id>')::bigint
                 AND c.row_data @> '{"<region>":"APAC"}'::jsonb
WHERE d.row_data @> '{"<status>":"open"}'::jsonb
GROUP BY dim_0;
```

`kind:` accepts `"inner"`, `"left"`, `"right"`, `"full"`. Default is `"inner"`.

### 18.5 `has` / `not_has` — semi-join (EXISTS)

For "rows that **have** at least one matching row in another table" without joining the other table's columns:

```latticeql
table("Contacts")
  | filter((c) -> {
      has(table("Deals"), where: (d) -> { d.owner_id == c.row_number and d.status == "won" })
    })
  | aggregate(count())
```

```sql
SELECT COUNT(*) AS measure
FROM rows c
WHERE c.table_id = (...)
  AND EXISTS (
    SELECT 1 FROM rows d
    WHERE d.table_id = (... 'Deals' ...)
      AND d.row_data @> '{"<owner_id>":' || c.row_number::text || ',"<status>":"won"}'::jsonb
  );
```

`not_has` compiles to `NOT EXISTS`. Use this instead of `join` when you don't need columns from the other side — PG can short-circuit on first match.

### 18.6 Choosing between primitives

| Need | Use |
|---|---|
| Each row + 1-2 fields from a referenced row | `lookup` |
| Filter parent set by something on the joined side, no aggregation across | `has` / `not_has` |
| Aggregate across the joined table (count contacts per stage) | `join` |
| Many-to-many via junction table | `join` (twice through the junction) |
| Self-reference, single level | `lookup(table("X"), r.parent)` |
| Multi-level tree within depth cap | `lookup_recursive` |

### 18.7 Depth & blast-radius limits

| Limit | Default | Configurable |
|---|---|---|
| `lookup` / `join` chain depth | 3 | per workspace |
| `lookup_recursive max_depth` | required, capped at 8 | per workspace |
| Cartesian without join condition | rejected at compile time | no |

---

## 19. Set operations

For combining same-shape pipelines (CRM segmentation, audit diffs):

```latticeql
won_this_q  := table("Deals") | filter((r) -> { r.status == "won" and r.closed_at >= "2026-04-01" })
won_last_q  := table("Deals") | filter((r) -> { r.status == "won" and r.closed_at >= "2026-01-01" and r.closed_at < "2026-04-01" })

// Owners who won in both quarters
won_this_q
  | project(@{ "owner": r.owner })
  | intersect(won_last_q | project(@{ "owner": r.owner }))
  | aggregate(count())
```

`union`, `union_all`, `intersect`, `except` all require both sides to expose the **same column shape** — use `project` to project to a common shape first. Compiles to PG `UNION` / `INTERSECT` / `EXCEPT` over the projected subqueries.

`union` dedupes; `union_all` does not. Prefer `union_all` when shapes are guaranteed disjoint — much cheaper.

---

## 20. Dashboard parameters — `$param`

Grafana-style global filters (time range, user, region) bind at panel-render time:

```latticeql
table("Tasks")
  | filter((r) -> {
      r.created_at >= $start_date and
      r.created_at <  $end_date and
      r.assignee   == $user
    })
  | group_by((r) -> { r.created_at | bucket($bucket_unit) })
  | aggregate(count())
```

The dashboard config declares parameters:

```jsonc
{
  "params": {
    "start_date":  { "type": "date",   "default": "now-30d" },
    "end_date":    { "type": "date",   "default": "now"     },
    "user":        { "type": "string", "default": null      },
    "bucket_unit": { "type": "enum",   "values": ["day", "week", "month"], "default": "day" }
  }
}
```

The compiler:

1. Validates each `$name` against declared type.
2. Substitutes `$bucket_unit` (a literal control parameter) inline.
3. Binds `$start_date` / `$end_date` / `$user` as **prepared-statement parameters** ($2, $3, $4 after the implicit `$1 = workspace_id`) — never string-interpolated. Date relative expressions (`"now-30d"`) resolve to absolute UTC instants at render time, before binding.

Result: same compiled SQL, different bound values per render — PG plan cache hits.

---

## 21. Row-list query (no aggregate)

For list / table panels without aggregation:

```latticeql
table("Tasks")
  | filter((r) -> { r.status == "blocked" })
  | sort_asc("created_at")
  | limit(20)
```

```sql
SELECT row_number, row_data
FROM rows
WHERE table_id = (...)
  AND row_data @> '{"<status>":"blocked"}'::jsonb
ORDER BY (row_data->>'<created_at>')::timestamptz ASC
LIMIT 20;
```

Output is the raw row list (`row_number` + full `row_data`). Frontend renders columns according to panel config.

---

## 22. Performance guardrails

The compiler emits **warnings** (not errors) for patterns that would scan more than necessary. They show in the panel editor and the query plan output.

| Trigger | Warning | Suggested fix |
|---|---|---|
| Range filter on a column with no expression index | "Range scan on `<col>` will full-scan." | Add B-tree expression index, or upgrade column type from `text` to `number`/`date`. |
| `contains` / `icontains` / regex on text column | "Substring scan on `<col>`." | Add `pg_trgm` GIN index; or use `starts_with` (B-tree) for prefix. |
| `unnest` followed by no aggregate | "Each row exploded N times into result set." | Add `aggregate` or `limit`. |
| Set operation without `union_all` when shapes are obviously disjoint | "`union` deduplicates — costs an extra sort." | Switch to `union_all`. |
| Window function on > 100k base rows | "Window over large set; consider filtering first." | Add `filter` before window. |
| `lookup_recursive` without `max_depth` | hard error, not warning | Always specify. |

Compiler also blocks:

- Join chain depth > workspace cap
- Cross-workspace queries
- Recursion without `max_depth`
- String interpolation in literals (SQL-injection vector)

---

## 23. Compile rules — full cheatsheet

### Filter clauses

| LatticeQL | SQL | Index |
|---|---|---|
| `r.x == V` (alone) | `row_data @> '{"<x>":V}'` | GIN ✅ |
| `r.x == V1 and r.y == V2 ...` | Single `@>` with multiple keys | GIN ✅ (flattening) |
| `r.x != V` | `NOT (row_data @> '{"<x>":V}')` | partial scan |
| `r.x in @[V1, V2]` (select) | `@> {x:V1} OR @> {x:V2}` | GIN ✅ (Bitmap OR) |
| `V in r.tags` | `row_data @> '{"<tags>":[V]}'` | GIN ✅ |
| `r.x < V` (number) | `(row_data->>'<x>')::numeric < V` | B-tree expr ✅ |
| `r.x < V` (date, UTC) | `(row_data->>'<x>')::timestamptz < V::timestamptz` | B-tree expr ✅ |
| `r.x != none` | `row_data ? '<x>'` | GIN ✅ |
| `r.x \| contains(V)` | `position(V in row_data->>'<x>') > 0` | seq scan or trigram |
| `r.x \| starts_with(V)` | `row_data->>'<x>' LIKE 'V%'` | B-tree if prefix indexed |
| `r.x \| matches(V)` | `row_data->>'<x>' ~ V` | seq scan |

### Pipeline stages

| LatticeQL | SQL |
|---|---|
| `table("X")` | `FROM rows WHERE table_id = (... 'X' ... workspace_id=$1)` |
| `\| filter(...)` (before agg) | `WHERE ...` |
| `\| with_column("c", lambda)` | inline `CASE WHEN ...` or computed expr |
| `\| group_by((r) -> { r.x })` | `GROUP BY row_data->>'<x>' AS dim_N` |
| `\| group_by((r) -> { r.x \| bucket("month") })` | `GROUP BY date_trunc('month', x AT TIME ZONE 'UTC')` |
| `\| aggregate(count())` | `COUNT(*) AS measure` |
| `\| aggregate(@{"a": e1, "b": e2})` | `e1 AS a, e2 AS b` |
| `\| aggregate(@{"a": sum_if(p, x)})` | `SUM(x) FILTER (WHERE p) AS a` |
| `\| filter(...)` (after agg) | `HAVING ...` |
| `\| with_window("c", lambda)` | wraps in subquery, `OVER (...)` |
| `\| sort_desc("name")` / `sort_asc("name")` | `ORDER BY name DESC/ASC` |
| `\| limit(N)` / `\| offset(N)` | `LIMIT N OFFSET N` |
| `\| distinct((r) -> { r.x })` | `DISTINCT ON (...)` |
| `\| unnest(r.tags, as: "t")` | `CROSS JOIN LATERAL jsonb_array_elements_text(...) t` |
| `\| project(@{...})` | `SELECT ... AS subquery` |
| `\| union(other)` / `intersect` / `except` | `UNION` / `INTERSECT` / `EXCEPT` |
| `\| lookup(table("X"), key)` | `LEFT JOIN rows ... ON row_number = key` |
| `\| lookup_recursive(...)` | `WITH RECURSIVE ... WHERE depth < N` |
| `\| join(other, kind:, on:)` | `<KIND> JOIN rows ... ON ...` |
| `\| has(other, where:)` / `not_has` | `EXISTS (...)` / `NOT EXISTS (...)` |
| `name := pipeline` | inline expansion at use sites |

### Built-ins

| LatticeQL | SQL |
|---|---|
| `r.x \| bucket(unit)` | `date_trunc(unit, x AT TIME ZONE 'UTC')` |
| `match v { p1 -> e1; ... _ -> ed }` | `CASE v WHEN p1 THEN e1 ... ELSE ed END` |
| `match v { _ if c1 -> e1; ... _ -> ed }` | `CASE WHEN c1 THEN e1 ... ELSE ed END` (guard form, §13) |
| `$param` | prepared-statement bind ($N) |
| Window: `agg(w.x) [partition_by w.p] [order_by w.o] [over_all]` | `agg(x) OVER (PARTITION BY p ORDER BY o)` |

---

## 24. Out of scope

LatticeQL is read-only and dashboard-focused. The following are **rejected at compile time**:

| Excluded | Reason |
|---|---|
| Mutation (`::=`, mutable params) | SQL is declarative; writes go through REST |
| Loops (`while`, `for`) | DoS risk |
| **Unbounded** recursion | DoS risk; bounded `lookup_recursive` is allowed (§18.3) |
| Side effects (`print`) | SELECT has no side-effect channel |
| String interpolation `${...}` | SQL injection vector — use `concat()` (§11.3) |
| Arbitrary PG function calls | Whitelist only: bucket, aggregates, lookup, join, match, window, unnest, set ops, string ops |
| Writes (INSERT / UPDATE / DELETE) | LatticeQL is read-only |
| Cross-workspace queries | Compiler rejects |
| Joins beyond depth cap | Default 3, configurable |
| Local-timezone literals or `at_timezone(...)` | All time is UTC (§1.4) |

---

## 25. Full example — Sprint Health dashboard

```latticeql
// === shared base ===
sprint_open := table("Tasks")
  | filter((r) -> {
      r.sprint == $sprint and
      not (r.status in @["merged", "done"])
    })

sprint_all := table("Tasks")
  | filter((r) -> { r.sprint == $sprint })


// === panel: completion rate (gauge) ===
completion := sprint_all
  | aggregate(@{
      "rate": avg_if((r) -> { r.status in @["done", "merged"] },
                     match r.status { "done" -> 1; "merged" -> 1; _ -> 0 })
    })


// === panel: backlog by priority × type (stacked bar) ===
backlog_by_priority := sprint_open
  | group_by(@{ "priority": (r) -> { r.priority }, "type": (r) -> { r.type } })
  | aggregate(count())


// === panel: daily burn-up with cumulative line ===
burnup := sprint_all
  | filter((r) -> { r.status in @["done", "merged"] })
  | group_by((r) -> { r.completed_at | bucket("day") })
  | aggregate(@{
      "completed":    count(),
      "story_points": sum(r.estimate)
    })
  | with_window("cumulative_pts", (w) -> { sum(w.story_points) order_by w.dim_0 })
  | sort_asc("dim_0")


// === panel: blockers by epic (recursive parent walk) ===
blockers_by_epic := sprint_open
  | filter((r) -> { r.status == "blocked" })
  | with_column("epic_title", (r) -> {
      lookup_recursive(table("Tasks"),
                       parent_key: r.parent,
                       field: "title",
                       max_depth: 5)
    })
  | unnest(r.epic_title, as: "epic")
  | group_by((r) -> { r.epic })
  | aggregate(count())
  | sort_desc("measure")


// === panel: workload top 5 + rank-in-team ===
workload := sprint_open
  | group_by(@{ "team": (r) -> { r.team }, "assignee": (r) -> { r.assignee } })
  | aggregate(@{ "tickets": count(), "estimate": sum(r.estimate) })
  | with_window("rank_in_team", (w) -> {
      rank() partition_by w.team order_by w.estimate desc
    })
  | filter((g) -> { g.rank_in_team <= 3 })
  | sort_desc("estimate")


// === panel: cycle-time p50/p95 (stat) ===
cycle_time := sprint_all
  | filter((r) -> { r.status == "merged" })
  | with_column("cycle_h", (r) -> { (r.completed_at - r.created_at) / 3600 })
  | aggregate(@{
      "p50": median(r.cycle_h),
      "p95": percentile(r.cycle_h, 0.95)
    })
```

Dashboard config:

```jsonc
{
  "name": "Sprint Health",
  "type": "dashboard",
  "params": {
    "sprint": { "type": "string", "default": "2026-W17" }
  },
  "config": {
    "grid": [
      { "panel": "completion",          "x": 0, "y": 0, "w": 3, "h": 3 },
      { "panel": "cycle_time",          "x": 3, "y": 0, "w": 3, "h": 3 },
      { "panel": "backlog_by_priority", "x": 6, "y": 0, "w": 6, "h": 3 },
      { "panel": "burnup",              "x": 0, "y": 3, "w": 8, "h": 4 },
      { "panel": "workload",            "x": 8, "y": 3, "w": 4, "h": 4 },
      { "panel": "blockers_by_epic",    "x": 0, "y": 7, "w": 12, "h": 3 }
    ],
    "panels": {
      "completion":          { "type": "gauge", "src": "completion.lql",          "thresholds": [0.3, 0.7] },
      "cycle_time":          { "type": "stat",  "src": "cycle_time.lql" },
      "backlog_by_priority": { "type": "bar",   "src": "backlog.lql",             "stack": "type" },
      "burnup":              { "type": "line",  "src": "burnup.lql",              "lines": ["completed", "cumulative_pts"] },
      "workload":            { "type": "bar",   "src": "workload.lql" },
      "blockers_by_epic":    { "type": "table", "src": "blockers_by_epic.lql" }
    }
  }
}
```

---

## 26. Mental model

```
┌──────────────────────────────────────────────────┐
│ 1. SOURCE     table("X")                         │
│              [+ join / lookup / lookup_recursive]│
├──────────────────────────────────────────────────┤
│ 2. PIPELINE  | filter         (WHERE)            │
│              | with_column                        │
│              | unnest                             │
│              | group_by                           │
│              | aggregate (incl. *_if conditional) │
│              | filter         (HAVING)            │
│              | with_window                        │
│              | sort | limit | offset              │
├──────────────────────────────────────────────────┤
│ 3. SET OPS    | union | intersect | except        │
├──────────────────────────────────────────────────┤
│ 4. SHAPE     [ {dim_0|named, ..., m_1, m_2, ...} ]│
│              → consumed by any chart type         │
└──────────────────────────────────────────────────┘
```

Reading top-to-bottom: **which table → join? → filter → group → measure → window → sort/limit**. Each pipe stage maps to one SQL clause; window stages add an outer query.

---

## 27. Column-type × operation lookup

| Column type | `==` | `<` / `>` | `in @[...]` | `contains` / regex | `!= none` |
|---|---|---|---|---|---|
| `select` | `@> {x:V}` | — | OR-of-`@>` | — | `?` |
| `text` | `@> {x:V}` | — | OR-of-`@>` | seq scan / trigram | `?` |
| `tags` (array) | (use `V in r.x`) | — | (use `V in r.x`) | — | `?` |
| `number` | `@> {x:V}` exact / `(::numeric)=V` | `(::numeric) <=>` | `(::numeric) IN (...)` | — | `?` |
| `date` (UTC `timestamptz`) | `@> {x:V}` / `(::timestamptz)=V` | `(::timestamptz) <=>` | — | — | `?` |
| `bool` | `@> {x:V}` | — | — | — | `?` |
| `url` | `@> {x:V}` | — | — | seq scan | `?` |
| `doc` | (not queryable — content lives in MinIO) | — | — | — | — |

**Rule:** when the value is a JSON literal (string / number / bool / null), prefer `@>` containment to hit the GIN index. Only fall back to `->>` + cast when range comparison or arithmetic is required.

---

## 28. One-page cheat sheet

```latticeql
// Full pipeline anatomy
[base :=]                                              // optional named base
  table("Tasks")                                       // 1. source
  | join(table("Other"), kind:, on:)                   //   optional join
  | with_column("c", (r) -> { lookup(...) })           //   optional lookup
  | unnest(r.tags, as: "tag")                          //   optional array explode
  | filter((r) -> { r.x == "..." and ... })            // 2. WHERE — flat ==/and for GIN
  | with_column("score", (r) -> { match ... })         // 3. virtual column
  | group_by(@{ "team": (r) -> { r.team } })           // 4. GROUP BY (1+, name or positional)
  | group_by((r) -> { r.date | bucket("month") })      //    UTC time dim
  | aggregate(@{                                       // 5. measures (incl. *_if, percentile)
      "n":          count(),
      "won_value":  sum_if((r) -> { r.status == "won" }, r.amount),
      "p95_cycle":  percentile(r.cycle_h, 0.95)
    })
  | filter((g) -> { g.n > 10 })                        // 6. HAVING
  | with_window("cum", (w) -> { sum(w.n) order_by w.dim_0 })   // 7. window
  | sort_desc("n")                                     // 8. ORDER BY
  | limit(10) | offset(0)                              // 9. paginate
```

**Five rules for fast LatticeQL:**

1. **Filter with flat `==`-and-`and` chains** → compiler flattens to a single `@>`, GIN handles it in one shot.
2. **`filter` position decides WHERE vs HAVING** → before `aggregate` is row-level; after is group-level.
3. **All time is UTC** → no `at_timezone`, all `bucket` boundaries are UTC, frontend handles display.
4. **Hoist shared filters with `:=`** → multiple panels share a base, each compiles to independent SQL for parallel execution.
5. **Reach for `*_if` over post-`filter`** when you need multiple conditional measures in one query — one scan, many measures.

Master these and your dashboards stay fast, readable, and easy to evolve.

---

## 29. Compiler usage — `lattice-ql`

`lattice-ql` reads a `.lql` panel source and emits parameterized PostgreSQL.

```bash
lattice-ql panel.lql --schema schema.json -o panel.sql --emit-params
```

| Flag | Meaning |
|---|---|
| `--schema <FILE>` | JSON column-metadata for the workspace |
| `--workspace <UUID>` | Inject as `$1`; otherwise stays a placeholder |
| `-o <FILE>` | Write SQL to file (else stdout) |
| `--emit-params` | Also write `<output>.sql.params.json` |
| `--explain` | Print AST + IR to stderr before SQL |

v0.1 supports the MVP subset listed in `.tmp/llm.design.compiler.md` §11.
Joins, windows, set ops, and unnest are v0.2.
