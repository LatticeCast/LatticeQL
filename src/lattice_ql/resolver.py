from __future__ import annotations

from .ast import (
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
from .error import SchemaError
from .schema import Schema


class Resolver:
    """Resolves human column names → column_ids in the AST."""

    def __init__(self, schema: Schema) -> None:
        self._schema = schema
        self._current_table: str | None = None
        self._in_having: bool = False
        self._virtual_cols: dict[str, object] = {}

    def resolve(self, query: Query) -> Query:
        if not query.stages or not isinstance(query.stages[0], TableStage):
            raise SchemaError("Query must start with table(...)")
        self._current_table = query.stages[0].name
        if self._schema.lookup_table(self._current_table) is None:
            raise SchemaError(f"Unknown table: {self._current_table!r}")

        seen_aggregate = False
        resolved = []
        for stage in query.stages:
            if isinstance(stage, (FilterStage, HavingStage)) and (
                seen_aggregate or isinstance(stage, HavingStage)
            ):
                self._in_having = True
                resolved.append(self._stage(stage))
                self._in_having = False
            else:
                resolved.append(self._stage(stage))
            if isinstance(stage, AggregateStage):
                seen_aggregate = True
        return Query(stages=resolved)

    def _stage(self, stage: object) -> object:
        if isinstance(stage, TableStage):
            return stage
        if isinstance(stage, FilterStage):
            return FilterStage(lambda_=self._lambda(stage.lambda_))
        if isinstance(stage, HavingStage):
            return HavingStage(lambda_=self._lambda(stage.lambda_))
        if isinstance(stage, GroupByStage):
            if isinstance(stage.dims, dict):
                return GroupByStage(dims={k: self._lambda(v) for k, v in stage.dims.items()})
            return GroupByStage(dims=[self._lambda(v) for v in stage.dims])
        if isinstance(stage, AggregateStage):
            if isinstance(stage.measures, dict):
                return AggregateStage(measures={k: self._agg(v) for k, v in stage.measures.items()})
            return AggregateStage(measures=self._agg(stage.measures))
        if isinstance(stage, SortStage):
            return SortStage(keys=[SortKey(self._expr(k.expr), k.desc) for k in stage.keys])
        if isinstance(stage, LimitStage):
            return stage
        if isinstance(stage, WithColumnStage):
            resolved_body = self._expr(stage.lambda_.body)
            self._virtual_cols[stage.name] = resolved_body
            return WithColumnStage(
                name=stage.name,
                lambda_=Lambda(param=stage.lambda_.param, body=resolved_body),
            )
        raise SchemaError(f"Unknown stage type: {type(stage)}")

    def _lambda(self, lam: Lambda) -> Lambda:
        return Lambda(param=lam.param, body=self._expr(lam.body))

    def _agg(self, agg: AggExpr) -> AggExpr:
        return AggExpr(
            func=agg.func,
            args=[self._expr(a) for a in agg.args],
            filter_lambda=self._lambda(agg.filter_lambda) if agg.filter_lambda else None,
        )

    def _expr(self, expr: object) -> object:
        if isinstance(expr, FieldAccess):
            if self._in_having:
                return expr  # aggregate alias reference — skip schema lookup
            if expr.field in self._virtual_cols:
                return expr  # virtual column — codegen will inline
            col = self._schema.lookup_column(self._current_table, expr.field)  # type: ignore[arg-type]
            if col is None:
                raise SchemaError(
                    f"Column {expr.field!r} not found in table {self._current_table!r}"
                )
            return FieldAccess(obj=expr.obj, field=col.id)
        if isinstance(expr, BinOp):
            return BinOp(expr.op, self._expr(expr.left), self._expr(expr.right))
        if isinstance(expr, UnaryOp):
            return UnaryOp(expr.op, self._expr(expr.operand))
        if isinstance(expr, InExpr):
            return InExpr(self._expr(expr.expr), [self._expr(i) for i in expr.items])
        if isinstance(expr, MatchExpr):
            arms = [
                MatchArm(
                    pattern=self._expr(a.pattern) if a.pattern is not None else None,
                    result=self._expr(a.result),
                )
                for a in expr.arms
            ]
            return MatchExpr(subject=self._expr(expr.subject), arms=arms)
        if isinstance(expr, PipeExpr):
            return PipeExpr(left=self._expr(expr.left), right=self._func(expr.right))
        if isinstance(expr, FuncCall):
            return self._func(expr)
        if isinstance(expr, Literal):
            return expr
        if isinstance(expr, ParamExpr):
            return expr
        raise SchemaError(f"Unknown expr type: {type(expr)}")

    def _func(self, expr: object) -> object:
        if isinstance(expr, FuncCall):
            return FuncCall(name=expr.name, args=[self._expr(a) for a in expr.args])
        raise SchemaError(f"Expected FuncCall, got {type(expr)}")
