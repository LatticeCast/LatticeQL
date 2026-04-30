from __future__ import annotations

from typing import Union

from .codegen import Codegen
from .parser import parse
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
    query = parse(lql)
    resolved = Resolver(schema).resolve(query)
    return Codegen(schema).generate(resolved)
