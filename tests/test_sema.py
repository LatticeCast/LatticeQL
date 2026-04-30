"""Unit tests for the Sema pass: GIN flattening and semantic validation."""

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
    Lambda,
    Literal,
    ParamExpr,
    PipeExpr,
    Query,
    TableStage,
    UnaryOp,
    WithColumnStage,
)
from lattice_ql.error import SemaError
from lattice_ql.schema import Schema
from lattice_ql.sema import Sema

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
}


def _schema() -> Schema:
    return Schema.from_dict(SCHEMA_DICT)


def _sema(query: Query) -> Query:
    return Sema(_schema()).transform(query)


def _query(*stages: object) -> Query:
    return Query(stages=list(stages))


def _lam(body: object, param: str = "r") -> Lambda:
    return Lambda(param=param, body=body)


def _fa(field: str) -> FieldAccess:
    return FieldAccess(obj="r", field=field)


# ---------------------------------------------------------------------------
# GIN flattening — single equality
# ---------------------------------------------------------------------------


def test_single_string_eq_becomes_gin_containment() -> None:
    """A single field==str in filter context → GinContainment."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("col-status"), Literal("todo")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, GinContainment)
    assert body.pairs == [("col-status", "todo")]


# ---------------------------------------------------------------------------
# GIN flattening — AND-chains
# ---------------------------------------------------------------------------


def test_two_string_eq_anded_merged_into_single_gin() -> None:
    """Two field==str AND-ed → single GinContainment with both pairs."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                BinOp(
                    "&&",
                    BinOp("==", _fa("col-status"), Literal("todo")),
                    BinOp("==", _fa("col-priority"), Literal("high")),
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, GinContainment)
    assert set(body.pairs) == {("col-status", "todo"), ("col-priority", "high")}


def test_three_string_eq_anded_merged() -> None:
    """Three field==str AND-ed (nested) → single GinContainment with all three pairs."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                BinOp(
                    "&&",
                    BinOp(
                        "&&",
                        BinOp("==", _fa("col-status"), Literal("in_progress")),
                        BinOp("==", _fa("col-priority"), Literal("high")),
                    ),
                    BinOp("==", _fa("col-sprint"), Literal("s1")),
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, GinContainment)
    assert len(body.pairs) == 3
    assert ("col-status", "in_progress") in body.pairs
    assert ("col-priority", "high") in body.pairs
    assert ("col-sprint", "s1") in body.pairs


def test_mixed_and_partial_gin() -> None:
    """str equality AND non-string condition → GinContainment AND other (BinOp)."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                BinOp(
                    "&&",
                    BinOp("==", _fa("col-status"), Literal("todo")),
                    BinOp(">", _fa("col-estimate"), Literal(5)),
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, BinOp)
    assert body.op == "&&"
    assert isinstance(body.left, GinContainment)
    assert body.left.pairs == [("col-status", "todo")]
    # The non-GIN condition passes through
    assert isinstance(body.right, BinOp)
    assert body.right.op == ">"


# ---------------------------------------------------------------------------
# GIN flattening — OR does NOT merge
# ---------------------------------------------------------------------------


def test_or_not_merged_into_single_gin() -> None:
    """String equalities OR-ed → each becomes its own GinContainment, NOT merged."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(
            lambda_=_lam(
                BinOp(
                    "||",
                    BinOp("==", _fa("col-status"), Literal("todo")),
                    BinOp("==", _fa("col-priority"), Literal("high")),
                )
            )
        ),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, BinOp)
    assert body.op == "||"
    # Each side becomes its own GinContainment
    assert isinstance(body.left, GinContainment)
    assert isinstance(body.right, GinContainment)
    assert body.left.pairs == [("col-status", "todo")]
    assert body.right.pairs == [("col-priority", "high")]


# ---------------------------------------------------------------------------
# GIN flattening — non-GIN cases pass through unchanged
# ---------------------------------------------------------------------------


def test_param_eq_not_gin() -> None:
    """field == $param → NOT converted to GinContainment."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("==", _fa("col-sprint"), ParamExpr(name="sprint")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, BinOp)
    assert body.op == "=="


def test_neq_not_gin() -> None:
    """field != str → NOT converted to GinContainment."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(BinOp("!=", _fa("col-status"), Literal("merged")))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, BinOp)
    assert body.op == "!="


def test_not_unary_recurses() -> None:
    """NOT(field==str) → NOT(GinContainment)."""
    q = _query(
        TableStage(name="Tasks"),
        FilterStage(lambda_=_lam(UnaryOp("!", BinOp("==", _fa("col-status"), Literal("done"))))),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    result = _sema(q)
    body = result.stages[1].lambda_.body  # type: ignore[union-attr]
    assert isinstance(body, UnaryOp)
    assert isinstance(body.operand, GinContainment)


# ---------------------------------------------------------------------------
# GIN flattening — conditional aggregate filter lambdas
# ---------------------------------------------------------------------------


def test_conditional_agg_filter_gin_flattened() -> None:
    """count_if filter lambda with string equality → GinContainment."""
    filter_lam = _lam(BinOp("==", _fa("col-status"), Literal("done")))
    agg = AggExpr(func="count_if", args=[], filter_lambda=filter_lam)
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures={"done_cnt": agg}),
    )
    result = _sema(q)
    stage = result.stages[1]
    assert isinstance(stage, AggregateStage)
    assert isinstance(stage.measures, dict)
    fl = stage.measures["done_cnt"].filter_lambda
    assert fl is not None
    assert isinstance(fl.body, GinContainment)
    assert fl.body.pairs == [("col-status", "done")]


def test_sum_if_filter_gin_flattened() -> None:
    """sum_if filter lambda → GinContainment."""
    filter_lam = _lam(
        BinOp(
            "&&",
            BinOp("==", _fa("col-status"), Literal("done")),
            BinOp("==", _fa("col-priority"), Literal("high")),
        )
    )
    agg = AggExpr(func="sum_if", args=[_fa("col-estimate")], filter_lambda=filter_lam)
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures={"total": agg}),
    )
    result = _sema(q)
    assert isinstance(result.stages[1], AggregateStage)
    assert isinstance(result.stages[1].measures, dict)
    fl = result.stages[1].measures["total"].filter_lambda
    assert fl is not None
    assert isinstance(fl.body, GinContainment)
    assert len(fl.body.pairs) == 2


# ---------------------------------------------------------------------------
# Type validation — numeric aggregates
# ---------------------------------------------------------------------------


def test_sum_on_number_column_ok() -> None:
    """sum on number column → no error."""
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="sum", args=[_fa("col-estimate")])),
    )
    _sema(q)  # must not raise


def test_sum_on_select_column_raises() -> None:
    """sum on select column → SemaError mentioning 'number'."""
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="sum", args=[_fa("col-status")])),
    )
    with pytest.raises(SemaError, match="number"):
        _sema(q)


def test_avg_on_text_column_raises() -> None:
    """avg on text column → SemaError."""
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="avg", args=[_fa("col-sprint")])),
    )
    with pytest.raises(SemaError, match="number"):
        _sema(q)


def test_median_on_date_raises() -> None:
    """median on date column → SemaError (date is not number)."""
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="median", args=[_fa("col-created-at")])),
    )
    with pytest.raises(SemaError, match="number"):
        _sema(q)


def test_sum_if_on_select_raises() -> None:
    """sum_if with select column as value → SemaError."""
    agg = AggExpr(
        func="sum_if",
        args=[_fa("col-status")],
        filter_lambda=_lam(BinOp("==", _fa("col-priority"), Literal("high"))),
    )
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures={"x": agg}),
    )
    with pytest.raises(SemaError, match="number"):
        _sema(q)


def test_count_on_any_column_ok() -> None:
    """count() takes no column argument — no type validation needed."""
    q = _query(
        TableStage(name="Tasks"),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    _sema(q)  # must not raise


# ---------------------------------------------------------------------------
# Type validation — bucket
# ---------------------------------------------------------------------------


def test_bucket_on_date_column_ok() -> None:
    """bucket on date column → no error."""
    pipe = PipeExpr(
        left=_fa("col-created-at"),
        right=FuncCall(name="bucket", args=[Literal("month")]),
    )
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(pipe)]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    _sema(q)  # must not raise


def test_bucket_on_text_column_raises() -> None:
    """bucket on text column → SemaError mentioning 'date'."""
    pipe = PipeExpr(
        left=_fa("col-sprint"),
        right=FuncCall(name="bucket", args=[Literal("month")]),
    )
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(pipe)]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    with pytest.raises(SemaError, match="date"):
        _sema(q)


def test_bucket_on_select_column_raises() -> None:
    """bucket on select column → SemaError."""
    pipe = PipeExpr(
        left=_fa("col-status"),
        right=FuncCall(name="bucket", args=[Literal("month")]),
    )
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(pipe)]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    with pytest.raises(SemaError, match="date"):
        _sema(q)


def test_bucket_in_with_column_date_ok() -> None:
    """bucket in with_column on date column → no error."""
    pipe = PipeExpr(
        left=_fa("col-created-at"),
        right=FuncCall(name="bucket", args=[Literal("week")]),
    )
    q = _query(
        TableStage(name="Tasks"),
        WithColumnStage(name="week", lambda_=_lam(pipe)),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    _sema(q)  # must not raise


def test_bucket_in_with_column_text_raises() -> None:
    """bucket in with_column on text column → SemaError."""
    pipe = PipeExpr(
        left=_fa("col-sprint"),
        right=FuncCall(name="bucket", args=[Literal("week")]),
    )
    q = _query(
        TableStage(name="Tasks"),
        WithColumnStage(name="week", lambda_=_lam(pipe)),
        AggregateStage(measures=AggExpr(func="count", args=[])),
    )
    with pytest.raises(SemaError, match="date"):
        _sema(q)


# ---------------------------------------------------------------------------
# Structure validation — having requires aggregate
# ---------------------------------------------------------------------------


def test_having_without_aggregate_raises() -> None:
    """having() with no prior aggregate stage → SemaError."""
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(_fa("col-status"))]),
        HavingStage(lambda_=_lam(BinOp(">", _fa("measure"), Literal(5)))),
    )
    with pytest.raises(SemaError, match="having"):
        _sema(q)


def test_having_with_aggregate_ok() -> None:
    """having() after aggregate → no error."""
    q = _query(
        TableStage(name="Tasks"),
        GroupByStage(dims=[_lam(_fa("col-status"))]),
        AggregateStage(measures=AggExpr(func="count", args=[])),
        HavingStage(lambda_=_lam(BinOp(">", _fa("measure"), Literal(5)))),
    )
    _sema(q)  # must not raise
