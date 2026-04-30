from __future__ import annotations

from .ast import (
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
    PipeExpr,
    Query,
    TableStage,
    UnaryOp,
    WithColumnStage,
)
from .error import SemaError
from .schema import ColumnKind, Schema

_NUMERIC_FUNCS = frozenset({"sum", "avg", "median", "percentile", "stddev", "variance"})


class Sema:
    """Semantic analysis: GIN flattening and validation of resolved AST."""

    def __init__(self, schema: Schema) -> None:
        self._schema = schema
        self._col_kind: dict[str, ColumnKind] = {}

    def transform(self, query: Query) -> Query:
        table_stage = query.stages[0]
        assert isinstance(table_stage, TableStage)
        table_meta = self._schema.lookup_table(table_stage.name)
        if table_meta:
            self._col_kind = {col.id: col.kind for col in table_meta.columns.values()}
        self._check_pipeline(query)
        return Query(stages=[self._stage(s) for s in query.stages])

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def _check_pipeline(self, query: Query) -> None:
        has_agg = any(isinstance(s, AggregateStage) for s in query.stages)
        for stage in query.stages:
            if isinstance(stage, HavingStage) and not has_agg:
                raise SemaError("having() requires a prior aggregate() in the pipeline")

    # ------------------------------------------------------------------
    # Stage dispatch
    # ------------------------------------------------------------------

    def _stage(self, stage: object) -> object:
        if isinstance(stage, FilterStage):
            body = self._gin_flatten(stage.lambda_.body)
            return FilterStage(lambda_=Lambda(param=stage.lambda_.param, body=body))
        if isinstance(stage, AggregateStage):
            if isinstance(stage.measures, dict):
                return AggregateStage(measures={k: self._agg(v) for k, v in stage.measures.items()})
            return AggregateStage(measures=self._agg(stage.measures))
        if isinstance(stage, GroupByStage):
            if isinstance(stage.dims, dict):
                for lam in stage.dims.values():
                    self._check_bucket(lam.body)
            else:
                for lam in stage.dims:
                    self._check_bucket(lam.body)
        if isinstance(stage, WithColumnStage):
            self._check_bucket(stage.lambda_.body)
        return (
            stage  # TableStage, GroupByStage, HavingStage, SortStage, LimitStage, WithColumnStage
        )

    # ------------------------------------------------------------------
    # Aggregate validation
    # ------------------------------------------------------------------

    def _agg(self, agg: AggExpr) -> AggExpr:
        func = agg.func.replace("_if", "")
        if func in _NUMERIC_FUNCS and agg.args:
            col = agg.args[0]
            if isinstance(col, FieldAccess):
                kind = self._col_kind.get(col.field)
                if kind is not None and kind != ColumnKind.number:
                    raise SemaError(
                        f"Aggregate {agg.func!r} requires a 'number' column,"
                        f" but column {col.field!r} has type {kind.value!r}"
                    )
        if agg.filter_lambda is not None:
            body = self._gin_flatten(agg.filter_lambda.body)
            fl = Lambda(param=agg.filter_lambda.param, body=body)
            return AggExpr(func=agg.func, args=agg.args, filter_lambda=fl)
        return agg

    def _check_bucket(self, expr: object) -> None:
        if (
            isinstance(expr, PipeExpr)
            and isinstance(expr.right, FuncCall)
            and expr.right.name == "bucket"
            and isinstance(expr.left, FieldAccess)
        ):
            kind = self._col_kind.get(expr.left.field)
            if kind is not None and kind != ColumnKind.date:
                raise SemaError(
                    f"bucket() requires a 'date' column,"
                    f" but column {expr.left.field!r} has type {kind.value!r}"
                )

    # ------------------------------------------------------------------
    # GIN flattening
    # ------------------------------------------------------------------

    def _gin_flatten(self, expr: object) -> object:
        """In filter context, fold AND-chains of (col_id == str) into GinContainment."""
        if isinstance(expr, BinOp) and expr.op == "&&":
            pairs, others = self._collect_gin_pairs(expr)
            return self._rebuild_and(pairs, others)
        if isinstance(expr, BinOp) and expr.op == "||":
            return BinOp("||", self._gin_flatten(expr.left), self._gin_flatten(expr.right))
        if isinstance(expr, UnaryOp) and expr.op == "!":
            return UnaryOp("!", self._gin_flatten(expr.operand))
        pair = self._as_gin_pair(expr)
        if pair is not None:
            return GinContainment([pair])
        return expr

    def _collect_gin_pairs(self, expr: object) -> tuple[list[tuple[str, str]], list[object]]:
        """Collect GIN-compatible pairs from an AND-chain without crossing OR boundaries."""
        if isinstance(expr, BinOp) and expr.op == "&&":
            lp, lo = self._collect_gin_pairs(expr.left)
            rp, ro = self._collect_gin_pairs(expr.right)
            return lp + rp, lo + ro
        pair = self._as_gin_pair(expr)
        if pair is not None:
            return [pair], []
        return [], [self._gin_flatten(expr)]

    def _as_gin_pair(self, expr: object) -> tuple[str, str] | None:
        if (
            isinstance(expr, BinOp)
            and expr.op == "=="
            and isinstance(expr.left, FieldAccess)
            and isinstance(expr.right, Literal)
            and isinstance(expr.right.value, str)
        ):
            return (expr.left.field, expr.right.value.replace("'", "''"))
        return None

    def _rebuild_and(self, pairs: list[tuple[str, str]], others: list[object]) -> object:
        conditions: list[object] = []
        if pairs:
            conditions.append(GinContainment(pairs))
        conditions.extend(others)
        if len(conditions) == 1:
            return conditions[0]
        result = conditions[0]
        for c in conditions[1:]:
            result = BinOp("&&", result, c)
        return result
