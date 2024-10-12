import regex
from dataclasses import dataclass

from .internal import *
from .common import *
from .ctoken import *
from .statement import *
from .variable import *
from .workspace import *

reg_define = regex.compile(
    r"^\#define\s++(?P<name>\w++)(?P<args>\((?P<args_in>[^)]*+)\))?\s*+(?P<body>.*)$", re_flags)
reg_whole_word = regex.compile(r"[\w\.]++", re_flags)

# The difference from re_token is that # and ## are operators rather than preprocessor directives
re_token_preproc = r'''(?(DEFINE)(?<TOKEN>
    (?> \/\/ (?: [^\\\n] | \\. )*+ \n) |
    (?> \/\* (?: [^*] | \*[^\/] )*+ \*\/ ) |
    (?> " (?> [^\\"] | \\. )* " ) |
    (?> ' (?> [^\\'] | \\. )* ' ) |
    (?> \{ (?&TOKEN)* \} ) |
    (?> \( (?&TOKEN)* \) ) |
    (?> \[ (?&TOKEN)* \] ) |
    (?>\n) |
    [\r\t ]++ |
    (?>\\.) |
    (?> , | ; | \? | : |
        ! | \~ |
        <<= | >>= |
        \#\# | \+\+ | \-\- | \-> | \+\+ | \-\- | << | >> | <= | >= | == | != |
        \&\& | \|\| | \+= | \-= | \*= | /= | %= | \&= | \^= | \|= |
        \# | \. | \+ | \- | \* | \& | / | % | \+ | \- | < | > |
        \& | \^ | \| | = |
        \@ # invalid charachter
    ) |
    \w++
))''' # /nxs;

reg_token_preproc = regex.compile(r"(?&TOKEN)"+re_token_preproc, re_flags)

def is_wellformed(txt: str) -> bool:
    offset = 0
    for match in reg_token_preproc.finditer(txt):
        if match.start() != offset:
            return False
        offset = match.end()
    return offset == len(txt)

@dataclass
class MacroParts:
    name: Token
    args: list[Token] | None = None
    body: Token | None = None
    preComment: Token | None = None
    postComment: Token | None = field(default=None, repr=False) # for compatibility with all details
    is_va_args: bool = False
    is_wellformed: bool = True
    # is_multiple_statements: bool = False
    is_const: bool | None = None
    typename: TokenList = field(default_factory=TokenList, repr=False) # compatibility with details

    # TODO(later): Parse body into a list of tokens.
    #              Use special token types for # and ## operators and replacements

    def __post_init__(self):
        if not self.body:
            self.is_wellformed = True
            self.is_const = True
        else:
            self.is_wellformed = is_wellformed(self.body.value)
            if not self.is_wellformed:
                self.is_const = False
            else:
                for token in TokenList.xxFilterCode(TokenList.xFromText(
                                self.body.value, base_offset=self.body.range[0])):
                    if token.getKind() in [" ", "#", "/"]:
                        continue
                    if (self.is_const is None and (
                            token.getKind() == "'" or
                            (token.getKind() == "w" and regex.match(r"^\d", token.value)))):
                        self.is_const = True
                    else:
                        self.is_const = False
                        break
                else:  # Not break
                    self.is_const = True

    def args_short_repr(self) -> str:
        return "(" + ", ".join([arg.value for arg in self.args]) + ")" if self.args else ""
    def short_repr(self) -> str:
        return (f"Macro {self.name.value}{self.args_short_repr()} "
                f"is_wellformed={self.is_wellformed} is_const={self.is_const}")

    def kind(self) -> str:
        return "macro"

    def update(self, other: 'MacroParts') -> list[str]:
        errors = []
        if self.name != other.name:
            errors.append(f"macro name mismatch for '{self.name.value}': "
                          f"'{self.name.value}' != '{other.name.value}'")
        if self.args != other.args:
            errors.append(f"macro args mismatch for '{self.name.value}': "
                          f"{self.args_short_repr()} != {other.args_short_repr()}")
        if self.body != other.body:
            errors.append(f"macro redifinition: '{self.name.value}'")
        if self.preComment is None:
            self.preComment = other.preComment
        return errors

    @staticmethod
    def fromStatement(statement: Statement) -> 'MacroParts | None':
        preComment = None
        for token in statement.tokens:
            if not preComment and token.getKind() == "/":
                preComment = token
                continue
            if token.getKind() in [" ", "/"]:
                continue
            if match := reg_define.match(token.value):
                break
            return None
        else: # not break
            return None

        is_va_args = False
        offset = token.range[0]
        args = None
        if match["args"]:
            args = list(
            TokenList.xFromMatches(reg_whole_word.finditer(match["args_in"]),
                                   offset + match.start("args_in"), kind="w"))
            if args and args[-1].value == "...":
                args[-1].value = "__VA_ARGS__"
                is_va_args = True

        body = Token.fromMatch(match, offset, "body")
        # space to preserve byte offset
        body.value = clean_text_sz(body.value.replace("\\\n", " \n").strip())

        return MacroParts(preComment=preComment, args=args, is_va_args=is_va_args,
            name=Token.fromMatch(match, offset, "name", kind="w"), # type: ignore # match is not None; match is indexable
            body=body) # type: ignore # match is not None; match is indexable
