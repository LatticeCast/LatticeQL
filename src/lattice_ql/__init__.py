from __future__ import annotations

from pathlib import Path
from typing import Union

from .ast import Query, VarRefStage
from .codegen import Codegen, CompiledQuery
from .error import ParseError
from .parser import parse_program
from .resolver import Resolver
from .schema import Schema, load_schema
from .sema import Sema

__all__ = ["compile", "CompiledQuery", "Schema", "load_schema"]


def compile(lql: str, schema: Union[dict, str, Path, Schema]) -> CompiledQuery:
    """Compile a LatticeQL query string to PostgreSQL SQL.

    Args:
        lql: LatticeQL source text.
        schema: Schema as a dict, JSON string, or Schema object.

    Returns:
        The SQL plus the bindings the compiler supplies (none today: the table
        name is inlined). The host supplies the required ``:workspace_id``
        execution placeholder.
    """
    if not isinstance(schema, Schema):
        schema = load_schema(schema)
    prog = parse_program(lql)
    bindings = {b.name: b.query for b in prog.bindings}
    query = _expand_bindings(prog.query, bindings)
    resolved = Resolver(schema).resolve(query)
    checked = Sema(schema).transform(resolved)
    return CompiledQuery(sql=Codegen(schema).generate(checked), params={})


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
