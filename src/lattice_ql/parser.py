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
    LetBinding,
    LimitStage,
    Literal,
    MatchArm,
    MatchExpr,
    ParamExpr,
    PipeExpr,
    Program,
    Query,
    SortKey,
    SortStage,
    TableStage,
    UnaryOp,
    VarRefStage,
    WithColumnStage,
)
from .error import ParseError
from .lexer import TT, Token, tokenize


class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        t = self._tokens[self._pos]
        self._pos += 1
        return t

    def _expect(self, kind: TT) -> Token:
        t = self._advance()
        if t.kind != kind:
            raise ParseError(f"Expected {kind}, got {t.kind} ({t.value!r}) at pos {t.pos}")
        return t

    def _eat(self, kind: TT) -> bool:
        if self._peek().kind == kind:
            self._advance()
            return True
        return False

    # ------------------------------------------------------------------
    # top level
    # ------------------------------------------------------------------

    def parse(self) -> Query:
        prog = self.parse_program()
        if prog.bindings:
            raise ParseError(
                "variable bindings require parse_program(); not supported in compile()"
            )
        return prog.query

    def parse_program(self) -> Program:
        bindings: list[LetBinding] = []
        while self._is_let_binding():
            name = str(self._advance().value)  # consume IDENT
            self._advance()  # consume WALRUS (:=)
            bindings.append(LetBinding(name=name, query=self._parse_pipeline()))
        query = self._parse_pipeline()
        self._expect(TT.EOF)
        return Program(bindings=bindings, query=query)

    def _is_let_binding(self) -> bool:
        return (
            self._peek().kind == TT.IDENT
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].kind == TT.WALRUS
        )

    def _parse_pipeline(self) -> Query:
        stages = []
        stages.append(self._parse_stage())
        while self._eat(TT.PIPE):
            stages.append(self._parse_stage())
        return Query(stages=stages)

    def _parse_stage(self) -> object:
        t = self._peek()
        if t.kind != TT.IDENT:
            raise ParseError(f"Expected stage name at pos {t.pos}")
        # bare identifier not followed by '(' → variable reference (e.g. sprint_all)
        next_kind = (
            self._tokens[self._pos + 1].kind if self._pos + 1 < len(self._tokens) else TT.EOF
        )
        if next_kind != TT.LPAREN:
            self._advance()
            return VarRefStage(name=str(t.value))
        name = t.value
        self._advance()
        self._expect(TT.LPAREN)
        if name == "table":
            result = self._parse_table_stage()
        elif name == "filter":
            result = self._parse_filter_stage()
        elif name == "group_by":
            result = self._parse_group_by_stage()
        elif name == "aggregate":
            result = self._parse_aggregate_stage()
        elif name == "sort":
            result = self._parse_sort_stage()
        elif name == "sort_asc":
            result = self._parse_sort_named(desc=False)
        elif name == "sort_desc":
            result = self._parse_sort_named(desc=True)
        elif name == "limit":
            result = self._parse_limit_stage()
        elif name == "having":
            result = self._parse_having_stage()
        elif name == "with_column":
            result = self._parse_with_column_stage()
        else:
            raise ParseError(f"Unknown stage: {name!r} at pos {t.pos}")
        self._expect(TT.RPAREN)
        return result

    def _parse_with_column_stage(self) -> WithColumnStage:
        col_name = str(self._expect(TT.STRING).value)
        self._expect(TT.COMMA)
        lam = self._parse_lambda()
        return WithColumnStage(name=col_name, lambda_=lam)

    def _parse_table_stage(self) -> TableStage:
        name = self._expect(TT.STRING).value
        return TableStage(name=str(name))

    def _parse_filter_stage(self) -> FilterStage:
        lam = self._parse_lambda()
        return FilterStage(lambda_=lam)

    def _parse_having_stage(self) -> HavingStage:
        lam = self._parse_lambda()
        return HavingStage(lambda_=lam)

    def _parse_group_by_stage(self) -> GroupByStage:
        # named: @{ "key": lambda, ... } or positional: lambda
        if self._peek().kind == TT.AT:
            self._advance()
            self._expect(TT.LBRACE)
            named: dict[str, Lambda] = {}
            while self._peek().kind != TT.RBRACE:
                key = str(self._expect(TT.STRING).value)
                self._expect(TT.COLON)
                lam = self._parse_lambda()
                named[key] = lam
                self._eat(TT.COMMA)
            self._expect(TT.RBRACE)
            return GroupByStage(dims=named)
        else:
            lam = self._parse_lambda()
            return GroupByStage(dims=[lam])

    def _parse_aggregate_stage(self) -> AggregateStage:
        if self._peek().kind == TT.AT:
            self._advance()
            self._expect(TT.LBRACE)
            named: dict[str, AggExpr] = {}
            while self._peek().kind != TT.RBRACE:
                key = str(self._expect(TT.STRING).value)
                self._expect(TT.COLON)
                agg = self._parse_agg_expr()
                named[key] = agg
                self._eat(TT.COMMA)
            self._expect(TT.RBRACE)
            return AggregateStage(measures=named)
        else:
            agg = self._parse_agg_expr()
            return AggregateStage(measures=agg)

    def _parse_sort_named(self, desc: bool) -> SortStage:
        """sort_asc("alias") / sort_desc("alias") — sort by a named result column."""
        alias = str(self._expect(TT.STRING).value)
        return SortStage(keys=[SortKey(expr=Literal(value=alias), desc=desc)])

    def _parse_sort_stage(self) -> SortStage:
        keys: list[SortKey] = []
        while True:
            expr = self._parse_expr()
            desc = False
            if self._peek().kind == TT.IDENT and self._peek().value == "desc":
                self._advance()
                desc = True
            keys.append(SortKey(expr=expr, desc=desc))
            if not self._eat(TT.COMMA):
                break
        return SortStage(keys=keys)

    def _parse_limit_stage(self) -> LimitStage:
        n = self._expect(TT.NUMBER).value
        return LimitStage(n=int(n))  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # agg expressions
    # ------------------------------------------------------------------

    _AGG_FUNCS = {
        "count",
        "count_distinct",
        "count_if",
        "sum",
        "sum_if",
        "avg",
        "avg_if",
        "min",
        "max",
        "median",
        "percentile",
        "stddev",
        "variance",
    }

    def _parse_agg_expr(self) -> AggExpr:
        t = self._expect(TT.IDENT)
        func = str(t.value)
        if func not in self._AGG_FUNCS:
            raise ParseError(f"Unknown aggregate function: {func!r} at pos {t.pos}")
        self._expect(TT.LPAREN)
        args: list[object] = []
        filter_lambda = None
        if self._peek().kind != TT.RPAREN:
            # _if variants: first arg is a lambda filter
            if func.endswith("_if"):
                filter_lambda = self._parse_lambda()
                if self._eat(TT.COMMA):
                    args.append(self._parse_expr())
            elif func == "count":
                pass  # no args
            elif func == "percentile":
                args.append(self._parse_expr())
                self._expect(TT.COMMA)
                args.append(self._parse_expr())
            else:
                args.append(self._parse_expr())
        self._expect(TT.RPAREN)
        return AggExpr(func=func, args=args, filter_lambda=filter_lambda)

    # ------------------------------------------------------------------
    # lambda
    # ------------------------------------------------------------------

    def _parse_lambda(self) -> Lambda:
        self._expect(TT.LPAREN)
        param = str(self._expect(TT.IDENT).value)
        self._expect(TT.RPAREN)
        self._expect(TT.ARROW)
        self._expect(TT.LBRACE)
        body = self._parse_expr()
        self._expect(TT.RBRACE)
        return Lambda(param=param, body=body)

    # ------------------------------------------------------------------
    # expressions
    # ------------------------------------------------------------------

    def _parse_expr(self) -> object:
        return self._parse_or()

    def _parse_or(self) -> object:
        left = self._parse_and()
        while self._peek().kind == TT.OR:
            self._advance()
            left = BinOp("||", left, self._parse_and())
        return left

    def _parse_and(self) -> object:
        left = self._parse_compare()
        while self._peek().kind == TT.AND:
            self._advance()
            left = BinOp("&&", left, self._parse_compare())
        return left

    def _parse_compare(self) -> object:
        left = self._parse_pipe_expr()
        t = self._peek()
        if t.kind in (TT.EQ, TT.NEQ, TT.LT, TT.LTE, TT.GT, TT.GTE):
            op = str(t.value)
            self._advance()
            return BinOp(op, left, self._parse_pipe_expr())
        if t.kind == TT.IDENT and t.value == "in":
            self._advance()
            self._expect(TT.AT)
            self._expect(TT.LBRACKET)
            items = []
            while self._peek().kind != TT.RBRACKET:
                items.append(self._parse_expr())
                self._eat(TT.COMMA)
            self._expect(TT.RBRACKET)
            return InExpr(expr=left, items=items)
        return left

    def _parse_pipe_expr(self) -> object:
        left = self._parse_unary()
        while self._peek().kind == TT.PIPE:
            self._advance()
            right = self._parse_call()
            left = PipeExpr(left=left, right=right)
        return left

    def _parse_unary(self) -> object:
        t = self._peek()
        if t.kind == TT.NOT:
            self._advance()
            return UnaryOp("!", self._parse_unary())
        return self._parse_primary()

    def _parse_primary(self) -> object:
        t = self._peek()

        if t.kind == TT.IDENT and t.value == "match":
            return self._parse_match()

        if t.kind == TT.IDENT:
            self._advance()
            if self._peek().kind == TT.LPAREN:
                return self._parse_call_args(str(t.value))
            if self._peek().kind == TT.DOT:
                self._advance()
                field = str(self._expect(TT.IDENT).value)
                return FieldAccess(obj=str(t.value), field=field)
            return Literal(value=str(t.value))

        if t.kind == TT.STRING:
            self._advance()
            return Literal(value=str(t.value))

        if t.kind == TT.NUMBER:
            self._advance()
            return Literal(value=t.value)

        if t.kind == TT.BOOL:
            self._advance()
            return Literal(value=bool(t.value))

        if t.kind == TT.NULL:
            self._advance()
            return Literal(value=None)

        if t.kind == TT.UNDERSCORE:
            self._advance()
            return Literal(value="_")

        if t.kind == TT.LPAREN:
            self._advance()
            expr = self._parse_expr()
            self._expect(TT.RPAREN)
            return expr

        if t.kind == TT.PARAM:
            self._advance()
            return ParamExpr(name=str(t.value))

        raise ParseError(f"Unexpected token {t.kind} ({t.value!r}) at pos {t.pos}")

    def _parse_call(self) -> FuncCall:
        name = str(self._expect(TT.IDENT).value)
        return self._parse_call_args(name)

    def _parse_call_args(self, name: str) -> FuncCall:
        self._expect(TT.LPAREN)
        args = []
        while self._peek().kind != TT.RPAREN:
            args.append(self._parse_expr())
            self._eat(TT.COMMA)
        self._expect(TT.RPAREN)
        return FuncCall(name=name, args=args)

    def _parse_match(self) -> MatchExpr:
        self._expect(TT.IDENT)  # consume "match"
        subject = self._parse_expr()
        self._expect(TT.LBRACE)
        arms: list[MatchArm] = []
        while self._peek().kind != TT.RBRACE:
            if self._peek().kind == TT.UNDERSCORE:
                self._advance()
                pattern = None
            else:
                pattern = self._parse_primary()
            self._expect(TT.ARROW)
            result = self._parse_expr()
            self._eat(TT.SEMI)
            arms.append(MatchArm(pattern=pattern, result=result))
        self._expect(TT.RBRACE)
        return MatchExpr(subject=subject, arms=arms)


def parse(src: str) -> Query:
    tokens = tokenize(src)
    return Parser(tokens).parse()


def parse_program(src: str) -> Program:
    tokens = tokenize(src)
    return Parser(tokens).parse_program()
