"""Unit tests for codegen.py — operates on already-resolved, sema-checked AST."""

from __future__ import annotations

import pytest

from lattice_ql.ast import (
    AggExpr,
    AggregateStage,
    BinOp,
    FieldAccess,
    FilterStage,
    FuncCall,
    GinContainment,
    GroupByStage,
    HavingStage,
    InExpr,
    Lambda,
    LimitStage,
    Literal,
    MatchArm,
    MatchExpr,
    ParamExpr,
    PipeExpr,
    Query,
    SortKey,
    SortStage,
    TableStage,
    UnaryOp,
    WithColumnStage,
)
from lattice_ql.codegen import Codegen
from lattice_ql.error import CodegenError
from lattice_ql.schema import Schema


def _cg(query: Query) -> str:
    return Codegen(Schema.empty()).generate(query)


def _q(*stages) -> Query:
    return Query(stages=list(stages))


def _lam(body) -> Lambda:
    return Lambda(param="r", body=body)


def _fa(field: str) -> FieldAccess:
    return FieldAccess(obj="r", field=field)


# ---------------------------------------------------------------------------
# Basic SELECT / FROM / WHERE
# ---------------------------------------------------------------------------


def test_count_all():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("count", [])))
    expected = (
        "SELECT COUNT(*) AS measure\n"
        "FROM rows\n"
        "WHERE table_id = (SELECT table_id FROM tables"
        " WHERE table_name = 'Tasks' AND workspace_id = $1);"
    )
    assert _cg(q) == expected


def test_workspace_id_always_dollar_one():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("count", [])))
    assert "workspace_id = $1" in _cg(q)


def test_table_name_in_from_clause():
    q = _q(TableStage("Deals"), AggregateStage(measures=AggExpr("count", [])))
    assert "table_name = 'Deals'" in _cg(q)


def test_ends_with_semicolon():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("count", [])))
    assert _cg(q).endswith(";")


# ---------------------------------------------------------------------------
# Filters — WHERE conditions
# ---------------------------------------------------------------------------


def test_filter_gin_single_pair():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(GinContainment([("col-status", "todo")]))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert 'row_data @> \'{"col-status":"todo"}\'::jsonb' in _cg(q)


def test_filter_gin_multiple_pairs():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(GinContainment([("col-status", "in_progress"), ("col-priority", "high")]))
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert '"col-status":"in_progress","col-priority":"high"' in _cg(q)


def test_filter_neq_string():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp("!=", _fa("col-status"), Literal("merged")))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "(row_data->>'col-status' ) != ('merged')" in _cg(q)


def test_filter_param_eq():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp("==", _fa("col-sprint"), ParamExpr("sprint")))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "(row_data->>'col-sprint' ) = ($2)" in _cg(q)


def test_filter_and():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(
                BinOp(
                    "&&",
                    BinOp("!=", _fa("col-status"), Literal("merged")),
                    BinOp("!=", _fa("col-status"), Literal("cancelled")),
                )
            )
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "AND" in sql
    assert "(row_data->>'col-status' ) != ('merged')" in sql
    assert "(row_data->>'col-status' ) != ('cancelled')" in sql


def test_filter_or():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(
                BinOp(
                    "||",
                    GinContainment([("col-status", "todo")]),
                    GinContainment([("col-status", "in_progress")]),
                )
            )
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "OR" in _cg(q)


def test_filter_not():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(UnaryOp("!", GinContainment([("col-status", "merged")])))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "NOT (" in _cg(q)


def test_filter_in_expr():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(
                InExpr(
                    _fa("col-status"), [Literal("todo"), Literal("in_progress"), Literal("testing")]
                )
            )
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "row_data->>'col-status'  IN ('todo', 'in_progress', 'testing')" in _cg(q)


# ---------------------------------------------------------------------------
# GROUP BY
# ---------------------------------------------------------------------------


def test_group_by_positional_field():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "row_data->>'col-priority'  AS dim_0" in sql
    assert "GROUP BY dim_0" in sql


def test_group_by_named():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims={"priority": _lam(_fa("col-priority"))}),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "row_data->>'col-priority'  AS priority" in sql
    assert "GROUP BY priority" in sql


def test_group_by_two_dims():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority")), _lam(_fa("col-assignee"))]),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "dim_0" in sql
    assert "dim_1" in sql
    assert "GROUP BY dim_0, dim_1" in sql


def test_group_by_bucket_expression_single_space():
    pipe = PipeExpr(_fa("col-created-at"), FuncCall("bucket", [Literal("month")]))
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(pipe)]),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    prefix = "date_trunc('month', (row_data->>'col-created-at')::timestamptz AT TIME ZONE 'UTC')"
    assert f"{prefix} AS dim_0" in sql


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


def test_single_aggregate_named_measure():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("count", [])))
    assert "COUNT(*) AS measure" in _cg(q)


def test_named_aggregates():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(
            measures={
                "cnt": AggExpr("count", []),
                "total": AggExpr("sum", [_fa("col-estimate")]),
                "mean": AggExpr("avg", [_fa("col-estimate")]),
            }
        ),
    )
    sql = _cg(q)
    assert "COUNT(*) AS cnt" in sql
    assert "SUM((row_data->>'col-estimate')::numeric) AS total" in sql
    assert "AVG((row_data->>'col-estimate')::numeric) AS mean" in sql


def test_aggregate_min_max():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(
            measures={
                "lo": AggExpr("min", [_fa("col-estimate")]),
                "hi": AggExpr("max", [_fa("col-estimate")]),
            }
        ),
    )
    sql = _cg(q)
    assert "MIN((row_data->>'col-estimate')::numeric) AS lo" in sql
    assert "MAX((row_data->>'col-estimate')::numeric) AS hi" in sql


def test_aggregate_stddev_variance():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(
            measures={
                "sd": AggExpr("stddev", [_fa("col-estimate")]),
                "vr": AggExpr("variance", [_fa("col-estimate")]),
            }
        ),
    )
    sql = _cg(q)
    assert "STDDEV_POP((row_data->>'col-estimate')::numeric) AS sd" in sql
    assert "VAR_POP((row_data->>'col-estimate')::numeric) AS vr" in sql


def test_aggregate_median():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("median", [_fa("col-estimate")])))
    assert (
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (row_data->>'col-estimate')::numeric)"
        in _cg(q)
    )


def test_aggregate_percentile():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(measures=AggExpr("percentile", [_fa("col-estimate"), Literal(0.95)])),
    )
    assert (
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (row_data->>'col-estimate')::numeric)"
        in _cg(q)
    )


def test_aggregate_count_distinct():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(measures=AggExpr("count_distinct", [_fa("col-assignee")])),
    )
    assert "COUNT(DISTINCT row_data->>'col-assignee')" in _cg(q)


def test_aggregate_unknown_raises():
    q = _q(TableStage("Tasks"), AggregateStage(measures=AggExpr("xyzzy", [])))
    with pytest.raises(CodegenError, match="Unknown aggregate"):
        _cg(q)


# ---------------------------------------------------------------------------
# Conditional aggregates (_if family)
# ---------------------------------------------------------------------------


def test_count_if():
    filt = _lam(GinContainment([("col-deal-status", "won")]))
    q = _q(
        TableStage("Deals"),
        GroupByStage(dims=[_lam(_fa("col-deal-owner"))]),
        AggregateStage(measures={"won_count": AggExpr("count_if", [], filter_lambda=filt)}),
    )
    sql = _cg(q)
    assert (
        'COUNT(*) FILTER (WHERE row_data @> \'{"col-deal-status":"won"}\'::jsonb) AS won_count'
        in sql
    )


def test_sum_if():
    filt = _lam(GinContainment([("col-deal-status", "won")]))
    q = _q(
        TableStage("Deals"),
        GroupByStage(dims=[_lam(_fa("col-deal-owner"))]),
        AggregateStage(
            measures={"won_value": AggExpr("sum_if", [_fa("col-deal-amount")], filter_lambda=filt)}
        ),
    )
    sql = _cg(q)
    assert "SUM((row_data->>'col-deal-amount')::numeric) FILTER (WHERE" in sql
    assert "won_value" in sql


def test_avg_if_with_in_filter():
    filt = _lam(InExpr(_fa("col-status"), [Literal("done"), Literal("merged")]))
    q = _q(
        TableStage("Tasks"),
        AggregateStage(
            measures={
                "rate": AggExpr(
                    "avg_if",
                    [
                        MatchExpr(
                            subject=_fa("col-status"),
                            arms=[
                                MatchArm(Literal("done"), Literal(1)),
                                MatchArm(Literal("merged"), Literal(1)),
                                MatchArm(None, Literal(0)),
                            ],
                        )
                    ],
                    filter_lambda=filt,
                )
            }
        ),
    )
    sql = _cg(q)
    assert "AVG(" in sql
    assert "FILTER (WHERE" in sql
    assert "IN ('done', 'merged')" in sql


# ---------------------------------------------------------------------------
# HAVING
# ---------------------------------------------------------------------------


def test_having_stage():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(measures=AggExpr("count", [])),
        HavingStage(_lam(BinOp(">", _fa("measure"), Literal(5)))),
    )
    sql = _cg(q)
    assert "HAVING (measure) > (5)" in sql


def test_post_aggregate_filter_becomes_having():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(measures=AggExpr("count", [])),
        FilterStage(_lam(BinOp(">", _fa("measure"), Literal(5)))),
    )
    sql = _cg(q)
    assert "HAVING" in sql
    assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT
# ---------------------------------------------------------------------------


def test_sort_asc_by_alias():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(measures=AggExpr("count", [])),
        SortStage(keys=[SortKey(expr=Literal("dim_0"), desc=False)]),
    )
    assert "ORDER BY dim_0 ASC" in _cg(q)


def test_sort_desc_by_alias():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-assignee"))]),
        AggregateStage(measures={"tickets": AggExpr("count", [])}),
        SortStage(keys=[SortKey(expr=Literal("tickets"), desc=True)]),
    )
    assert "ORDER BY tickets DESC" in _cg(q)


def test_sort_multiple_keys():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority")), _lam(_fa("col-assignee"))]),
        AggregateStage(measures=AggExpr("count", [])),
        SortStage(
            keys=[
                SortKey(expr=Literal("dim_0"), desc=False),
                SortKey(expr=Literal("measure"), desc=True),
            ]
        ),
    )
    assert "ORDER BY dim_0 ASC, measure DESC" in _cg(q)


def test_limit():
    q = _q(
        TableStage("Tasks"),
        AggregateStage(measures=AggExpr("count", [])),
        LimitStage(n=10),
    )
    assert "LIMIT 10" in _cg(q)


# ---------------------------------------------------------------------------
# MATCH expression → CASE ... WHEN ... THEN ... END
# ---------------------------------------------------------------------------


def test_match_in_with_column_avg():
    match = MatchExpr(
        subject=_fa("col-priority"),
        arms=[
            MatchArm(Literal("critical"), Literal(100)),
            MatchArm(Literal("high"), Literal(50)),
            MatchArm(None, Literal(0)),
        ],
    )
    q = _q(
        TableStage("Tasks"),
        WithColumnStage("urgency", _lam(match)),
        GroupByStage(dims=[_lam(_fa("col-assignee"))]),
        AggregateStage(measures={"avg_urgency": AggExpr("avg", [_fa("urgency")])}),
    )
    sql = _cg(q)
    assert "CASE row_data->>'col-priority'" in sql
    assert "WHEN 'critical' THEN 100" in sql
    assert "WHEN 'high' THEN 50" in sql
    assert "ELSE 0" in sql
    assert "::numeric" in sql  # virtual column gets cast


def test_match_wildcard_else():
    match = MatchExpr(
        subject=_fa("col-status"),
        arms=[
            MatchArm(Literal("done"), Literal(1)),
            MatchArm(None, Literal(0)),
        ],
    )
    q = _q(
        TableStage("Tasks"),
        AggregateStage(measures=AggExpr("avg", [match])),
    )
    sql = _cg(q)
    assert "ELSE 0" in sql
    assert "WHEN 'done' THEN 1" in sql


# ---------------------------------------------------------------------------
# Parameters — numbering and deduplication
# ---------------------------------------------------------------------------


def test_first_named_param_is_dollar_two():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp("==", _fa("col-sprint"), ParamExpr("sprint")))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "$2" in sql


def test_two_distinct_params_numbered_sequentially():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(
                BinOp(
                    "&&",
                    BinOp("==", _fa("col-sprint"), ParamExpr("sprint")),
                    BinOp("==", _fa("col-assignee"), ParamExpr("user")),
                )
            )
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert "$2" in sql
    assert "$3" in sql


def test_same_param_reuses_placeholder():
    q = _q(
        TableStage("Tasks"),
        FilterStage(
            _lam(
                BinOp(
                    "||",
                    BinOp("==", _fa("col-sprint"), ParamExpr("sprint")),
                    BinOp("==", _fa("col-type"), ParamExpr("sprint")),
                )
            )
        ),
        AggregateStage(measures=AggExpr("count", [])),
    )
    sql = _cg(q)
    assert sql.count("$2") == 2
    assert "$3" not in sql


# ---------------------------------------------------------------------------
# Clause ordering
# ---------------------------------------------------------------------------


def test_clause_order_select_from_where_group_having_order_limit():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(GinContainment([("col-status", "todo")]))),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(measures=AggExpr("count", [])),
        HavingStage(_lam(BinOp(">", _fa("measure"), Literal(2)))),
        SortStage(keys=[SortKey(expr=Literal("measure"), desc=True)]),
        LimitStage(n=5),
    )
    sql = _cg(q)
    positions = {
        kw: sql.index(kw)
        for kw in ("SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT")
    }
    order = sorted(positions, key=lambda k: positions[k])
    assert order == ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_no_aggregate_no_group_selects_star():
    # Row-list query: no aggregate stage → SELECT *
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(GinContainment([("col-status", "todo")]))),
    )
    assert "SELECT *" in _cg(q)


def test_unknown_stage_raises():
    class BogusStage:
        pass

    q = Query(stages=[TableStage("Tasks"), BogusStage()])
    with pytest.raises(CodegenError, match="Unknown stage"):
        _cg(q)


def test_literal_null():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp("==", _fa("col-x"), Literal(None)))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "NULL" in _cg(q)


def test_literal_bool_true():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp("==", _fa("col-x"), Literal(True)))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "TRUE" in _cg(q)


def test_literal_numeric():
    q = _q(
        TableStage("Tasks"),
        FilterStage(_lam(BinOp(">", _fa("col-estimate"), Literal(42)))),
        AggregateStage(measures=AggExpr("count", [])),
    )
    assert "42" in _cg(q)


def test_multi_select_aligned_with_commas():
    q = _q(
        TableStage("Tasks"),
        GroupByStage(dims=[_lam(_fa("col-priority"))]),
        AggregateStage(
            measures={
                "cnt": AggExpr("count", []),
                "total": AggExpr("sum", [_fa("col-estimate")]),
            }
        ),
    )
    sql = _cg(q)
    lines = sql.splitlines()
    # first SELECT line ends with ","
    assert lines[0].endswith(",")
    # intermediate lines end with ","
    assert lines[1].endswith(",")
    # last select line does NOT end with ","
    assert not lines[2].endswith(",")
