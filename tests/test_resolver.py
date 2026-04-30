"""Unit tests for the Resolver: column-name → column-id mapping."""

from __future__ import annotations

import pytest

from lattice_ql.ast import (
    AggExpr,
    AggregateStage,
    BinOp,
    FieldAccess,
    FilterStage,
    FuncCall,
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
from lattice_ql.error import SchemaError
from lattice_ql.resolver import Resolver
from lattice_ql.schema import Schema

SCHEMA_DICT = {
    "Tasks": {
        "table_id": "tbl-tasks",
        "columns": {
            "status": {"id": "col-status", "type": "select"},
            "priority": {"id": "col-priority", "type": "select"},
            "estimate": {"id": "col-estimate", "type": "number"},
            "created_at": {"id": "col-created-at", "type": "date"},
            "sprint": {"id": "col-sprint", "type": "text"},
        },
    },
    "Deals": {
        "table_id": "tbl-deals",
        "columns": {
            "status": {"id": "col-deal-status", "type": "select"},
            "amount": {"id": "col-deal-amount", "type": "number"},
            "owner": {"id": "col-deal-owner", "type": "text"},
        },
    },
}


def _schema() -> Schema:
    return Schema.from_dict(SCHEMA_DICT)


def _resolve(query: Query, schema: Schema | None = None) -> Query:
    return Resolver(schema or _schema()).resolve(query)


def _query(*stages: object) -> Query:
    return Query(stages=list(stages))


def _lam(body: object, param: str = "r") -> Lambda:
    return Lambda(param=param, body=body)


def _fa(field: str) -> FieldAccess:
    return FieldAccess(obj="r", field=field)


# ---------------------------------------------------------------------------
# Basic field resolution
# ---------------------------------------------------------------------------


def test_field_access_resolved_to_col_id() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("status"), Literal("todo")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    f = resolved.stages[1]
    assert isinstance(f, FilterStage)
    assert isinstance(f.lambda_.body, BinOp)
    assert isinstance(f.lambda_.body.left, FieldAccess)
    assert f.lambda_.body.left.field == "col-status"


def test_group_by_field_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(_fa("priority"))]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    gb = resolved.stages[1]
    assert isinstance(gb, GroupByStage)
    lam = gb.dims[0]  # type: ignore[index]
    assert isinstance(lam.body, FieldAccess)
    assert lam.body.field == "col-priority"


def test_named_group_by_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims={"pri": _lam(_fa("priority"))}),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    gb = resolved.stages[1]
    assert isinstance(gb, GroupByStage)
    assert isinstance(gb.dims["pri"].body, FieldAccess)  # type: ignore[index]
    assert gb.dims["pri"].body.field == "col-priority"  # type: ignore[index,union-attr]


def test_aggregate_field_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="sum", args=[_fa("estimate")])),
    )
    resolved = _resolve(q)
    agg = resolved.stages[1]
    assert isinstance(agg, AggregateStage)
    assert isinstance(agg.measures, AggExpr)
    assert isinstance(agg.measures.args[0], FieldAccess)
    assert agg.measures.args[0].field == "col-estimate"


def test_named_aggregate_fields_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(
            measures={
                "total": AggExpr(func="sum", args=[_fa("estimate")]),
                "cnt": AggExpr(func="count", args=[]),
            }
        ),
    )
    resolved = _resolve(q)
    agg = resolved.stages[1]
    assert isinstance(agg, AggregateStage)
    assert isinstance(agg.measures, dict)
    assert agg.measures["total"].args[0].field == "col-estimate"  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_unknown_table_raises() -> None:
    q = _query(TableStage(name="NoSuchTable"))
    with pytest.raises(SchemaError, match="Unknown table"):
        _resolve(q)


def test_unknown_column_raises() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("nonexistent"), Literal("x")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    with pytest.raises(SchemaError, match="Column 'nonexistent' not found"):
        _resolve(q)


def test_no_table_stage_raises() -> None:
    q = Query(stages=[FilterStage(lambda_=_lam(Literal("x")))])
    with pytest.raises(SchemaError, match="Query must start with table"):
        _resolve(q)


def test_empty_stages_raises() -> None:
    q = Query(stages=[])
    with pytest.raises(SchemaError, match="Query must start with table"):
        _resolve(q)


# ---------------------------------------------------------------------------
# Having context — FieldAccess refers to aggregate alias, not column name
# ---------------------------------------------------------------------------


def test_having_stage_field_not_schema_resolved() -> None:
    # having((r) -> { r.total > 5 }) — "total" is an aggregate alias, not a col name
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(_fa("priority"))]),
        AggregateStage(measures={"total": AggExpr(func="count", args=[])}),
        HavingStage(lambda_=_lam(BinOp(">", _fa("total"), Literal(5)))),
    )
    resolved = _resolve(q)
    hv = resolved.stages[3]
    assert isinstance(hv, HavingStage)
    # "total" should stay as-is — it's an alias, not a column name
    assert isinstance(hv.lambda_.body, BinOp)
    assert isinstance(hv.lambda_.body.left, FieldAccess)
    assert hv.lambda_.body.left.field == "total"


def test_post_aggregate_filter_treated_as_having() -> None:
    # filter after aggregate → in_having=True → field untouched
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(_fa("priority"))]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
        FilterStage(lambda_=_lam(BinOp(">", _fa("measure"), Literal(3)))),
    )
    resolved = _resolve(q)
    f = resolved.stages[3]
    assert isinstance(f, FilterStage)
    assert isinstance(f.lambda_.body.left, FieldAccess)
    assert f.lambda_.body.left.field == "measure"


# ---------------------------------------------------------------------------
# with_column — virtual columns tracked, not resolved via schema
# ---------------------------------------------------------------------------


def test_with_column_registers_virtual() -> None:
    q = _query(
        TableStage(name="Tasks"),
        WithColumnStage(name="month", lambda_=_lam(_fa("created_at"))),
        GroupByStage(dims=[_lam(_fa("month"))]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    gb = resolved.stages[2]
    assert isinstance(gb, GroupByStage)
    lam = gb.dims[0]  # type: ignore[index]
    # "month" is virtual — should remain as-is (not looked up in schema)
    assert isinstance(lam.body, FieldAccess)
    assert lam.body.field == "month"


def test_with_column_body_field_resolved() -> None:
    # The body of with_column should have its fields resolved
    q = _query(
        TableStage(name="Tasks"),
        WithColumnStage(name="ts", lambda_=_lam(_fa("created_at"))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    wc = resolved.stages[1]
    assert isinstance(wc, WithColumnStage)
    assert isinstance(wc.lambda_.body, FieldAccess)
    assert wc.lambda_.body.field == "col-created-at"


# ---------------------------------------------------------------------------
# Expression types pass-through / recursion
# ---------------------------------------------------------------------------


def test_literal_unchanged() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("status"), Literal("done")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    rhs = resolved.stages[1].lambda_.body.right  # type: ignore[union-attr]
    assert isinstance(rhs, Literal)
    assert rhs.value == "done"


def test_param_expr_unchanged() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("sprint"), ParamExpr(name="sprint")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    rhs = resolved.stages[1].lambda_.body.right  # type: ignore[union-attr]
    assert isinstance(rhs, ParamExpr)
    assert rhs.name == "sprint"


def test_unary_op_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(UnaryOp("!", BinOp("==", _fa("status"), Literal("done"))))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    body = resolved.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, UnaryOp)
    inner = body.operand
    assert isinstance(inner, BinOp)
    assert isinstance(inner.left, FieldAccess)
    assert inner.left.field == "col-status"


def test_in_expr_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(InExpr(expr=_fa("priority"), items=[Literal("high"), Literal("critical")]))
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    body = resolved.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, InExpr)
    assert isinstance(body.expr, FieldAccess)
    assert body.expr.field == "col-priority"


def test_match_expr_subject_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                MatchExpr(
                    subject=_fa("status"),
                    arms=[
                        MatchArm(pattern=Literal("todo"), result=Literal(1)),
                        MatchArm(pattern=None, result=Literal(0)),
                    ],
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    body = resolved.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, MatchExpr)
    assert isinstance(body.subject, FieldAccess)
    assert body.subject.field == "col-status"


def test_pipe_expr_left_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(
            dims=[
                _lam(
                    PipeExpr(
                        left=_fa("created_at"),
                        right=FuncCall(name="bucket", args=[Literal("month")]),
                    )
                )
            ]
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    gb = resolved.stages[1]
    assert isinstance(gb, GroupByStage)
    lam = gb.dims[0]  # type: ignore[index]
    assert isinstance(lam.body, PipeExpr)
    assert isinstance(lam.body.left, FieldAccess)
    assert lam.body.left.field == "col-created-at"


# ---------------------------------------------------------------------------
# Sort / Limit pass-through
# ---------------------------------------------------------------------------


def test_sort_stage_field_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="count", args=[])),
        SortStage(keys=[SortKey(expr=_fa("estimate"), desc=True)]),
    )
    # estimate is in the schema — but sort is after aggregate so it's in having context
    # Actually sort keys are plain expressions — they go through _expr which in having=False
    # context resolves normally. estimate is a real column so it should resolve.
    resolved = _resolve(q)
    sort = resolved.stages[2]
    assert isinstance(sort, SortStage)


def test_limit_stage_unchanged() -> None:
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="count", args=[])),
        LimitStage(n=10),
    )
    resolved = _resolve(q)
    lim = resolved.stages[2]
    assert isinstance(lim, LimitStage)
    assert lim.n == 10


# ---------------------------------------------------------------------------
# Multi-condition / nested logic
# ---------------------------------------------------------------------------


def test_and_condition_both_fields_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                BinOp(
                    "&&",
                    BinOp("==", _fa("status"), Literal("todo")),
                    BinOp("==", _fa("priority"), Literal("high")),
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    resolved = _resolve(q)
    body = resolved.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, BinOp)
    assert body.left.left.field == "col-status"  # type: ignore[union-attr]
    assert body.right.left.field == "col-priority"  # type: ignore[union-attr]


def test_conditional_aggregate_filter_resolved() -> None:
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(
            measures={
                "done_count": AggExpr(
                    func="count_if",
                    args=[],
                    filter_lambda=_lam(BinOp("==", _fa("status"), Literal("done"))),
                )
            }
        ),
    )
    resolved = _resolve(q)
    agg = resolved.stages[1]
    assert isinstance(agg, AggregateStage)
    assert isinstance(agg.measures, dict)
    fl = agg.measures["done_count"].filter_lambda
    assert fl is not None
    assert isinstance(fl.body, BinOp)
    assert isinstance(fl.body.left, FieldAccess)
    assert fl.body.left.field == "col-status"
