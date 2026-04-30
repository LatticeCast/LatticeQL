from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Query:
    stages: list[Stage]


Stage = Any  # refined in parser


@dataclass
class TableStage:
    name: str


@dataclass
class FilterStage:
    lambda_: Lambda


@dataclass
class GroupByStage:
    # positional: list[Lambda] or named: dict[str, Lambda]
    dims: list[Lambda] | dict[str, Lambda]


@dataclass
class AggregateStage:
    # single expr or named dict
    measures: AggExpr | dict[str, AggExpr]


@dataclass
class SortStage:
    keys: list[SortKey]


@dataclass
class LimitStage:
    n: int


@dataclass
class HavingStage:
    lambda_: Lambda


# ---------- expressions ----------


@dataclass
class Lambda:
    param: str
    body: Expr


Expr = Any


@dataclass
class BinOp:
    op: str
    left: Expr
    right: Expr


@dataclass
class UnaryOp:
    op: str
    operand: Expr


@dataclass
class FieldAccess:
    obj: str  # always the lambda param for now
    field: str


@dataclass
class Literal:
    value: Any  # str | int | float | bool | None


@dataclass
class InExpr:
    expr: Expr
    items: list[Expr]


@dataclass
class MatchExpr:
    subject: Expr
    arms: list[MatchArm]


@dataclass
class MatchArm:
    pattern: Expr | None  # None = wildcard _
    result: Expr


@dataclass
class PipeExpr:
    left: Expr
    right: Expr  # typically a FuncCall like bucket(...)


@dataclass
class FuncCall:
    name: str
    args: list[Expr]


# ---------- aggregate expressions ----------


@dataclass
class AggExpr:
    func: str
    args: list[Expr]
    filter_lambda: Lambda | None = None


@dataclass
class SortKey:
    expr: Expr
    desc: bool = False
