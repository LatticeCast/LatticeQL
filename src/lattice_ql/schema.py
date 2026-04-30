from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Union

from .error import SchemaError


class ColumnKind(str, Enum):
    select = "select"
    text = "text"
    tags = "tags"
    number = "number"
    date = "date"
    bool = "bool"
    url = "url"
    doc = "doc"


@dataclass
class ColumnMeta:
    id: str
    kind: ColumnKind


@dataclass
class TableMeta:
    table_id: str
    columns: dict[str, ColumnMeta]


class Schema:
    def __init__(self, tables: dict[str, TableMeta]) -> None:
        self._tables = tables

    @classmethod
    def empty(cls) -> Schema:
        return cls({})

    @classmethod
    def from_dict(cls, data: dict) -> Schema:
        tables: dict[str, TableMeta] = {}
        for table_name, tdata in data.items():
            cols: dict[str, ColumnMeta] = {}
            for col_name, cdata in tdata["columns"].items():
                try:
                    kind = ColumnKind(cdata["type"])
                except ValueError as exc:
                    raise SchemaError(f"Unknown column type: {cdata['type']}") from exc
                cols[col_name] = ColumnMeta(id=cdata["id"], kind=kind)
            tables[table_name] = TableMeta(table_id=tdata["table_id"], columns=cols)
        return cls(tables)

    @classmethod
    def from_json(cls, text: str) -> Schema:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise SchemaError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    def lookup_column(self, table_name: str, col_name: str) -> ColumnMeta | None:
        table = self._tables.get(table_name)
        if table is None:
            return None
        return table.columns.get(col_name)

    def lookup_table(self, table_name: str) -> TableMeta | None:
        return self._tables.get(table_name)


def load_schema(schema: Union[dict, str]) -> Schema:
    if isinstance(schema, dict):
        return Schema.from_dict(schema)
    return Schema.from_json(schema)
