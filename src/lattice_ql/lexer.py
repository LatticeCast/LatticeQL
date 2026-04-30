from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto

from .error import LexError


class TT(Enum):
    # literals
    STRING = auto()
    NUMBER = auto()
    BOOL = auto()
    NULL = auto()
    # identifiers / keywords
    IDENT = auto()
    # operators
    PIPE = auto()  # |
    ARROW = auto()  # ->
    LBRACE = auto()  # {
    RBRACE = auto()  # }
    LPAREN = auto()  # (
    RPAREN = auto()  # )
    LBRACKET = auto()  # [
    RBRACKET = auto()  # ]
    AT = auto()  # @
    COMMA = auto()  # ,
    COLON = auto()  # :
    DOT = auto()  # .
    SEMI = auto()  # ;
    EQ = auto()  # ==
    NEQ = auto()  # !=
    LT = auto()  # <
    LTE = auto()  # <=
    GT = auto()  # >
    GTE = auto()  # >=
    AND = auto()  # &&
    OR = auto()  # ||
    NOT = auto()  # !
    UNDERSCORE = auto()  # _
    EOF = auto()


@dataclass
class Token:
    kind: TT
    value: object
    pos: int


_PATTERNS: list[tuple[str, TT | None]] = [
    (r"\s+", None),
    (r"//[^\n]*", None),
    (r'"(?:[^"\\]|\\.)*"', TT.STRING),
    (r"\d+(?:\.\d+)?", TT.NUMBER),
    (r"->", TT.ARROW),
    (r"==", TT.EQ),
    (r"!=", TT.NEQ),
    (r"<=", TT.LTE),
    (r">=", TT.GTE),
    (r"&&", TT.AND),
    (r"\|\|", TT.OR),
    (r"\|", TT.PIPE),
    (r"<", TT.LT),
    (r">", TT.GT),
    (r"\{", TT.LBRACE),
    (r"\}", TT.RBRACE),
    (r"\(", TT.LPAREN),
    (r"\)", TT.RPAREN),
    (r"\[", TT.LBRACKET),
    (r"\]", TT.RBRACKET),
    (r"@", TT.AT),
    (r",", TT.COMMA),
    (r":", TT.COLON),
    (r"\.", TT.DOT),
    (r";", TT.SEMI),
    (r"!", TT.NOT),
    (r"_\b", TT.UNDERSCORE),
    (r"[A-Za-z_][A-Za-z0-9_]*", TT.IDENT),
]

_KEYWORDS = {
    "true": TT.BOOL,
    "false": TT.BOOL,
    "null": TT.NULL,
    "_": TT.UNDERSCORE,
    "and": TT.AND,
    "or": TT.OR,
}

_MASTER = re.compile("|".join(f"({p})" for p, _ in _PATTERNS))


def tokenize(src: str) -> list[Token]:
    tokens: list[Token] = []
    pos = 0
    while pos < len(src):
        m = _MASTER.match(src, pos)
        if not m:
            raise LexError(f"Unexpected character {src[pos]!r} at position {pos}")
        pos = m.end()
        for i, (_, tt) in enumerate(_PATTERNS):
            if m.group(i + 1) is not None:
                if tt is None:
                    break
                raw = m.group(i + 1)
                if tt == TT.IDENT and raw in _KEYWORDS:
                    tt = _KEYWORDS[raw]
                    value: object = raw == "true" if tt == TT.BOOL else raw
                elif tt == TT.STRING:
                    value = raw[1:-1].encode().decode("unicode_escape")
                elif tt == TT.NUMBER:
                    value = float(raw) if "." in raw else int(raw)
                elif tt == TT.BOOL:
                    value = raw == "true"
                else:
                    value = raw
                tokens.append(Token(tt, value, m.start()))
                break
    tokens.append(Token(TT.EOF, None, pos))
    return tokens
