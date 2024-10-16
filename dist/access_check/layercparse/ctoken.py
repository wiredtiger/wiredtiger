import enum
import regex
from dataclasses import dataclass, field
from typing import Iterable

from . import common
from .internal import *

TokenKind: TypeAlias = Literal[
        "",   # undefined
        " ",  # space
        "/",  # comment
        "w",  # word
        "+",  # operator
        "'",  # string
        "(",  # ()
        "{",  # {}
        "[",  # []
        "#",  # preproc
        ";",  # end of expression: , or ;
        "@"]  # invalid thing

def getTokenKind(txt: str) -> TokenKind:
    return \
        " " if txt.startswith((" ", "\t", "\n", "\r")) else \
        "/" if txt.startswith(("//", "/*")) else \
        "'" if txt.startswith(("'", '"')) else \
        "(" if txt.startswith("(") else \
        "{" if txt.startswith("{") else \
        "[" if txt.startswith("[") else \
        "#" if txt.startswith("#") else \
        ";" if txt in [",", ";"] else \
        "+" if txt in c_ops_all else \
        "w" if reg_word_char.match(txt) else \
        "@" if txt.startswith("@") else \
        ""

@dataclass
class Token:
    """One token in the source code"""
    idx: int = field(compare=False)     # Index in the original stream of tokens
    range: Range = field(compare=False) # Character range in the original text
    value: str                          # Text value
    kind: TokenKind | None = field(default=None, repr=False)

    def getKind(self) -> TokenKind:
        if self.kind is not None:
            return self.kind
        self.kind = getTokenKind(self.value)
        return self.kind

    @staticmethod
    def fromMatch(match: regex.Match, base_offset: int = 0,
                  match_group: int | str = 0, idx: int = 0,
                  kind: TokenKind | None = None) -> 'Token':
        return Token(idx,
                     rangeShift(match.span(match_group), base_offset), match[match_group], kind)

    @staticmethod
    def empty() -> 'Token':
        return Token(0, (0, 0), "")

class TokenList(list[Token]):
    """List of tokens"""
    def range(self) -> Range:
        return (self[0].range[0], self[-1].range[1]) if len(self) > 0 else (0, 0)

    def strings(self) -> Iterable[str]:
        for t in self:
            yield t.value

    def short_repr(self) -> str:
        return " ".join(self.strings())

    @staticmethod
    def xFromMatches(matches: Iterable[regex.Match], base_offset: int = 0,
                     match_group: int | str = 0, kind: TokenKind | None = None) -> Iterable[Token]:
        i = 0
        for match in matches:
            yield Token.fromMatch(match, base_offset, match_group, idx=i, kind=kind)
            i += 1
    @staticmethod
    def xFromText(txt: str, base_offset: int, **kwargs) -> Iterable[Token]:
        i = 0
        for match in reg_token.finditer(txt, **kwargs):
            yield Token.fromMatch(match, base_offset=base_offset, idx=i)
            i += 1
    @staticmethod
    def fromText(txt: str, base_offset: int, **kwargs) -> 'TokenList':
        return TokenList(TokenList.xFromText(txt, base_offset=base_offset, **kwargs))

    @staticmethod
    def xFromFile(fname: str, **kwargs) -> Iterable[Token]:
        with open(fname) as file:
            return TokenList.xFromText(file.read(), base_offset=0, **kwargs)
    @staticmethod
    def fromFile(fname: str, **kwargs) -> 'TokenList':
        return TokenList(TokenList.xFromFile(fname, **kwargs))

    def __str__(self) -> str:
        return f"[{self.range()[0]}:{self.range()[1]}] 〈{'⌇'.join(self.strings())}〉"
    def __repr__(self) -> str:
        return f"[{self.range()[0]}:{self.range()[1]}] 〈{'⌇'.join(self.strings())}〉"

    @staticmethod
    def xxFilterCode(tokens: Iterable[Token]) -> Iterable[Token]:
        for t in tokens:
            if t.getKind() not in [" ", "#", "/", ";"]:
                yield t
    def xFilterCode(self) -> Iterable[Token]:
        return TokenList.xxFilterCode(self)
    def filterCode(self) -> 'TokenList':
        return TokenList(self.xFilterCode())

    def xFilterCode_r(self) -> Iterable[Token]:
        for t in reversed(self):
            if t.getKind() not in [" ", "#", "/", ";"]:
                yield t
    def filterCode_r(self) -> 'TokenList':
        return TokenList(self.xFilterCode_r())


def get_pre_comment(tokens: TokenList) -> tuple[Token | None, int]:
    for i in range(len(tokens)):
        token = tokens[i]
        if token.getKind() == " ":
            continue
        if token.getKind() == "/":
            return (token, i+1)
        return (None, i)
    return (None, i+1)


def get_post_comment(tokens: TokenList) -> Token | None:
    for token in reversed(tokens):
        if token.getKind() == " ":
            continue
        if token.getKind() == "/":
            return token
        return None
    return None

