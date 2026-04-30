from __future__ import annotations

from typing import Union

from .ast import Query, VarRefStage
from .codegen import Codegen
from .error import ParseError
from .parser import parse_program
from .resolver import Resolver
from .schema import Schema, load_schema

__all__ = ["compile", "Schema"]


def compile(lql: str, schema: Union[dict, str, Schema]) -> str:
    """Compile a LatticeQL query string to PostgreSQL SQL.

    Args:
        lql: LatticeQL source text.
        schema: Schema as a dict, JSON string, or Schema object.

    Returns:
        A PostgreSQL SQL string.
    """
    if not isinstance(schema, Schema):
        schema = load_schema(schema)
    prog = parse_program(lql)
    bindings = {b.name: b.query for b in prog.bindings}
    query = _expand_bindings(prog.query, bindings)
    resolved = Resolver(schema).resolve(query)
    return Codegen(schema).generate(resolved)


def _expand_bindings(query: Query, bindings: dict) -> Query:
    """Inline the first stage if it's a variable reference."""
    if not query.stages:
        return query
    first = query.stages[0]
    if isinstance(first, VarRefStage):
        if first.name not in bindings:
            raise ParseError(f"Undefined variable: {first.name!r}")
        bound = bindings[first.name]
        return Query(stages=bound.stages + query.stages[1:])
    return query
