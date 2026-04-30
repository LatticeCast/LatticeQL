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
    MatchExpr,
    PipeExpr,
    Query,
    SortKey,
    SortStage,
    TableStage,
    UnaryOp,
)
from .error import CodegenError
from .schema import Schema


class Codegen:
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def generate(self, query: Query) -> str:
        table_stage: TableStage = query.stages[0]  # type: ignore[assignment]
        rest = query.stages[1:]

        # filters before aggregate → WHERE; filters after aggregate → HAVING
        pre_filters: list[FilterStage] = []
        post_filters: list[FilterStage] = []
        group_by: GroupByStage | None = None
        aggregate: AggregateStage | None = None
        having: HavingStage | None = None
        sort: SortStage | None = None
        limit: LimitStage | None = None
        seen_aggregate = False

        for stage in rest:
            if isinstance(stage, FilterStage):
                if seen_aggregate:
                    post_filters.append(stage)
                else:
                    pre_filters.append(stage)
            elif isinstance(stage, GroupByStage):
                group_by = stage
            elif isinstance(stage, AggregateStage):
                aggregate = stage
                seen_aggregate = True
            elif isinstance(stage, HavingStage):
                having = stage
            elif isinstance(stage, SortStage):
                sort = stage
            elif isinstance(stage, LimitStage):
                limit = stage
            else:
                raise CodegenError(f"Unknown stage: {type(stage)}")

        table_name = table_stage.name
        parts: list[str] = []

        # SELECT — raw-field dims get 2 spaces before AS, expression dims get 1;
        # measures get 1; all items multi-line aligned
        select_parts: list[str] = []
        if group_by is not None:
            for alias, sql, is_field in self._dim_exprs(group_by):
                spaces = "  " if is_field else " "
                select_parts.append(f"{sql}{spaces}AS {alias}")
        if aggregate is not None:
            if isinstance(aggregate.measures, dict):
                for name, agg in aggregate.measures.items():
                    select_parts.append(f"{self._agg_sql(agg)} AS {name}")
            else:
                select_parts.append(f"{self._agg_sql(aggregate.measures)} AS measure")
        elif not select_parts:
            select_parts.append("*")

        indent = " " * 7  # len("SELECT ")
        if len(select_parts) == 1:
            parts.append(f"SELECT {select_parts[0]}")
        else:
            lines = [f"SELECT {select_parts[0]},"]
            for p in select_parts[1:-1]:
                lines.append(f"{indent}{p},")
            lines.append(f"{indent}{select_parts[-1]}")
            parts.append("\n".join(lines))

        # FROM + base WHERE
        parts.append(
            f"FROM rows\n"
            f"WHERE table_id = (SELECT table_id FROM tables"
            f" WHERE table_name = '{table_name}' AND workspace_id = $1)"
        )

        # pre-aggregate filters → WHERE AND clauses
        for f in pre_filters:
            parts.append(f"  AND {self._filter_cond(f.lambda_)}")

        # GROUP BY
        if group_by is not None:
            aliases = [alias for alias, _, _ in self._dim_exprs(group_by)]
            parts.append("GROUP BY " + ", ".join(aliases))

        # post-aggregate filters → HAVING
        for f in post_filters:
            parts.append(f"HAVING {self._having_cond(f.lambda_)}")
        if having is not None:
            parts.append(f"HAVING {self._having_cond(having.lambda_)}")

        # ORDER BY (only when explicitly requested)
        if sort is not None:
            parts.append("ORDER BY " + ", ".join(self._sort_key_sql(k) for k in sort.keys))

        if limit is not None:
            parts.append(f"LIMIT {limit.n}")

        return "\n".join(parts) + ";"

    # ------------------------------------------------------------------

    def _dim_exprs(self, gb: GroupByStage) -> list[tuple[str, str, bool]]:
        """Returns (alias, sql, is_raw_field) triples."""

        def entry(alias: str, lam: Lambda) -> tuple[str, str, bool]:
            sql = self._expr_sql(lam.body)
            is_field = isinstance(lam.body, FieldAccess)
            return alias, sql, is_field

        if isinstance(gb.dims, dict):
            return [entry(k, v) for k, v in gb.dims.items()]
        return [entry(f"dim_{i}", v) for i, v in enumerate(gb.dims)]

    def _filter_cond(self, lam: Lambda) -> str:
        return self._where_sql(lam.body)

    def _having_cond(self, lam: Lambda) -> str:
        return self._expr_sql(lam.body)

    # ------------------------------------------------------------------
    # WHERE expression — uses GIN @> for text equality
    # ------------------------------------------------------------------

    def _where_sql(self, expr: object) -> str:
        if isinstance(expr, BinOp):
            if expr.op == "==" and isinstance(expr.left, FieldAccess):
                if isinstance(expr.right, Literal) and isinstance(expr.right.value, str):
                    cid = expr.left.field
                    val = expr.right.value.replace("'", "''")
                    return f'row_data @> \'{{"{cid}":"{val}"}}\'::jsonb'
            if expr.op == "!=" and isinstance(expr.left, FieldAccess):
                if isinstance(expr.right, Literal) and isinstance(expr.right.value, str):
                    cid = expr.left.field
                    val = expr.right.value.replace("'", "''")
                    return f"(row_data->>'{cid}' ) != ('{val}')"
            if expr.op == "&&":
                merged = self._try_gin_merge(expr)
                if merged is not None:
                    return merged
                return f"({self._where_sql(expr.left)} AND {self._where_sql(expr.right)})"
            if expr.op == "||":
                return f"({self._where_sql(expr.left)} OR {self._where_sql(expr.right)})"
            return (
                f"{self._expr_sql(expr.left)} {self._sql_op(expr.op)} {self._expr_sql(expr.right)}"
            )
        if isinstance(expr, UnaryOp) and expr.op == "!":
            return f"NOT ({self._where_sql(expr.operand)})"
        if isinstance(expr, InExpr) and isinstance(expr.expr, FieldAccess):
            cid = expr.expr.field
            items_sql = ", ".join(
                f"'{item.value}'"
                if isinstance(item, Literal) and isinstance(item.value, str)
                else self._expr_sql(item)
                for item in expr.items
            )
            return f"row_data->>'{cid}'  IN ({items_sql})"
        return self._expr_sql(expr)

    def _try_gin_merge(self, expr: BinOp) -> str | None:
        """Merge a chain of (field == string) && conditions into a single @> containment."""
        pairs = self._collect_eq_pairs(expr)
        if pairs is None:
            return None
        body = ",".join(f'"{cid}":"{val}"' for cid, val in pairs)
        return f"row_data @> '{{{body}}}'::jsonb"

    def _collect_eq_pairs(self, expr: object) -> list[tuple[str, str]] | None:
        if isinstance(expr, BinOp) and expr.op == "&&":
            left = self._collect_eq_pairs(expr.left)
            right = self._collect_eq_pairs(expr.right)
            if left is None or right is None:
                return None
            return left + right
        if (
            isinstance(expr, BinOp)
            and expr.op == "=="
            and isinstance(expr.left, FieldAccess)
            and isinstance(expr.right, Literal)
            and isinstance(expr.right.value, str)
        ):
            return [(expr.left.field, expr.right.value.replace("'", "''"))]
        return None

    # ------------------------------------------------------------------
    # General expression → SQL
    # ------------------------------------------------------------------

    def _expr_sql(self, expr: object) -> str:
        if isinstance(expr, FieldAccess):
            return f"row_data->>'{expr.field}'"
        if isinstance(expr, Literal):
            v = expr.value
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, (int, float)):
                return str(v)
            return f"'{str(v).replace(chr(39), chr(39) * 2)}'"
        if isinstance(expr, BinOp):
            op = self._sql_op(expr.op)
            return f"({self._expr_sql(expr.left)} {op} {self._expr_sql(expr.right)})"
        if isinstance(expr, UnaryOp):
            return f"NOT ({self._expr_sql(expr.operand)})"
        if isinstance(expr, PipeExpr):
            return self._pipe_sql(expr)
        if isinstance(expr, FuncCall):
            raise CodegenError(f"Unknown function call: {expr.name!r}")
        if isinstance(expr, MatchExpr):
            return self._match_sql(expr)
        if isinstance(expr, InExpr):
            items = ", ".join(self._expr_sql(i) for i in expr.items)
            return f"{self._expr_sql(expr.expr)} IN ({items})"
        raise CodegenError(f"Cannot generate SQL for {type(expr)}")

    def _pipe_sql(self, expr: PipeExpr) -> str:
        if isinstance(expr.right, FuncCall) and expr.right.name == "bucket":
            unit = str(expr.right.args[0].value)  # type: ignore[union-attr]
            inner = self._expr_sql(expr.left)
            return f"date_trunc('{unit}', ({inner})::timestamptz AT TIME ZONE 'UTC')"
        raise CodegenError(f"Cannot pipe into {expr.right!r}")

    def _match_sql(self, expr: MatchExpr) -> str:
        subject = self._expr_sql(expr.subject)
        when_clauses: list[str] = []
        else_clause = ""
        for arm in expr.arms:
            if arm.pattern is None:
                else_clause = f" ELSE {self._expr_sql(arm.result)}"
            else:
                pat = self._expr_sql(arm.pattern)
                res = self._expr_sql(arm.result)
                when_clauses.append(f" WHEN {subject} = {pat} THEN {res}")
        return f"CASE{''.join(when_clauses)}{else_clause} END"

    # ------------------------------------------------------------------
    # Aggregates
    # ------------------------------------------------------------------

    def _agg_sql(self, agg: AggExpr) -> str:
        filter_clause = ""
        if agg.filter_lambda is not None:
            cond = self._filter_cond(agg.filter_lambda)
            filter_clause = f" FILTER (WHERE {cond})"

        func = agg.func.replace("_if", "")
        if func == "count":
            return f"COUNT(*){filter_clause}"
        if func == "count_distinct":
            col = self._expr_sql(agg.args[0])
            return f"COUNT(DISTINCT {col}){filter_clause}"
        if func in ("sum", "avg", "min", "max", "stddev", "variance"):
            col = self._numeric(agg.args[0])
            sql_func = {
                "sum": "SUM",
                "avg": "AVG",
                "min": "MIN",
                "max": "MAX",
                "stddev": "STDDEV_POP",
                "variance": "VAR_POP",
            }[func]
            return f"{sql_func}({col}){filter_clause}"
        if func == "median":
            col = self._numeric(agg.args[0])
            return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}){filter_clause}"
        if func == "percentile":
            col = self._numeric(agg.args[0])
            p = self._expr_sql(agg.args[1])
            return f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}){filter_clause}"
        raise CodegenError(f"Unknown aggregate: {agg.func!r}")

    def _numeric(self, expr: object) -> str:
        return f"({self._expr_sql(expr)})::numeric"

    def _sort_key_sql(self, key: SortKey) -> str:
        # sort_asc/sort_desc pass a Literal string alias — output unquoted
        if isinstance(key.expr, Literal) and isinstance(key.expr.value, str):
            sql = str(key.expr.value)
        else:
            sql = self._expr_sql(key.expr)
        return f"{sql} DESC" if key.desc else f"{sql} ASC"

    def _sql_op(self, op: str) -> str:
        return {
            "==": "=",
            "!=": "!=",
            "<": "<",
            "<=": "<=",
            ">": ">",
            ">=": ">=",
            "&&": "AND",
            "||": "OR",
        }.get(op, op)
