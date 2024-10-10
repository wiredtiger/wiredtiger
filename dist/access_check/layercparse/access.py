import regex
import multiprocessing
import itertools
from dataclasses import dataclass, field
from typing import Iterable, Any
from pprint import pformat

from .common import *
from .record import *
from .function import *
from .codebase import *
from .workspace import *

_reg_member_access_chain = regex.compile(r"""
    # ((?<!\w)\((?&TOKEN)++\))*                        # Possible type conversions - not needed
    (?>
        ([a-zA-Z_]\w*+)(?>(?>\((?&TOKEN)*+\))|(?>\[(?&TOKEN)*+\]))*+ |  # (1) variable or function call or array index
        (\((?&TOKEN)++\))             # (2) expression
    )
    (?>(?>->|\.)(?>([a-zA-Z_]\w*+)(?>\[(?&TOKEN)++\])?))++             # (3) member access chain via -> or .
"""+re_token, re_flags)

@dataclass
class AccessChain:
    name: str
    members: list[str]
    offset: int

    def __str__(self) -> str:
        return f"{self.name}->{'.'.join(self.members)}"

def member_access_chains(txt: str, offset_in_parent: int = 0) -> Iterable[AccessChain]:
    for match in _reg_member_access_chain.finditer(txt):
        offset = match.start() + offset_in_parent
        if match[1]:
            yield AccessChain(match[1], match.allcaptures()[3], offset)  # type: ignore[misc] # Tuple index out of range
        elif match[2]:
            yield AccessChain(match[2], match.allcaptures()[3], offset)  # type: ignore[misc] # Tuple index out of range
            yield from member_access_chains(match[2][1:-1], offset_in_parent + match.start(2) + 1)


@dataclass
class AccessCheck:
    _globals: Codebase
    _perModuleInvisibleNamesRe: dict[str, regex.Pattern | None] = field(default_factory=dict)

    def _get_invisible_global_names_for_module(self, module: str) -> regex.Pattern | None:
        if module not in self._perModuleInvisibleNamesRe:
            retSet = set()
            for name in self._globals.names_restricted:
                if self._globals.names_restricted[name].module != module:
                    retSet.add(name)
            if not retSet:
                self._perModuleInvisibleNamesRe[module] = None
            else:
                # reg_name =  r"(?<!->|\.)\s*+\b(" + "|".join(retSet) + r")\s*+(?:->|\.|\()"
                # reg_name =  r"(?<!->|\.)\s*+\b(" + "|".join(retSet) + r")\b"
                reg_name =  r"(?<!(?:->|\.)\s*+)(?:" + "|".join(retSet) + r")\b"
                self._perModuleInvisibleNamesRe[module] = regex.compile(reg_name, re_flags)
        return self._perModuleInvisibleNamesRe[module]

    def _check_function(self, defn: Definition) -> None:
        if defn.kind != "function" or \
                not defn.details or \
                not isinstance(defn.details, FunctionParts) or \
                not defn.details.body:
            return
        body_clean = clean_text_more_sz(defn.details.body.value)

        def _locationStr(offset: int) -> str:
            return (defn.scope.locationStr(defn.details.body.range[0] + offset) + # type: ignore[union-attr]
                    (f" [{module}]" if module else "") + f" '{defn.name}':")

        def _LOC(offset: int) -> Callable[[], str]:
            return lambda: _locationStr(offset)

        module = defn.module
        filename = defn.scope.file.name

        DEBUG5(_LOC(0), f"=== body:\n{body_clean}\n===")

        # Check local names
        localvars: dict[str, Definition] = {} # name -> type
        for var in defn.details.getArgs() + defn.details.getLocalVars(self._globals):
            if var.typename:
                localvars[var.name.value] = Definition(
                    name=var.name.value,
                    kind="variable",
                    scope=defn.scope,
                    offset=var.name.range[0],
                    module="",
                    is_private=False,
                    details=var)
            else:
                WARNING(_LOC(var.name.range[0]), f"Missing type for local variable '{var.name.value}'")

        if localvars:
            DEBUG4(_LOC(0), f"locals vars:\n{pformat(localvars, width=120, compact=False)}") or \
            DEBUG2(_LOC(0), f"locals vars:\n" + "\n".join(f"    {name}: {cast(Details, d.details).typename.short_repr()}" for name, d in localvars.items()))

        def _get_type_of_name(name: str) -> str:
            # Consider scopes in order: local, static, global
            if name in localvars:
                details = localvars[name].details
            elif filename in self._globals.static_names and name in self._globals.static_names[filename]:
                details = self._globals.static_names[filename][name].details
            elif name in self._globals.names:
                details = self._globals.names[name].details
            else:
                return ""
            return self._globals.untypedef(get_base_type(cast(Details, details).typename)) if details else ""

        def _get_type_of_expr(tokens: TokenList, root_offset: int = 0) -> str:
            while tokens and tokens[0].getKind() == "+":  # remove any prefix ops
                tokens.pop(0)
            if not tokens:
                return ""

            for i in range(len(tokens)):
                if tokens[i].value == "?":  # it's a "?:" operator - find the type of the 2nd and/or 3rd expression
                    # find the :
                    for j in range(i+1, len(tokens)):
                        if tokens[j].value == ":":
                            break
                    if ret := _get_type_of_expr(TokenList(tokens[i:j-1]), root_offset + tokens[i].range[0]):
                        return ret
                    return _get_type_of_expr(TokenList(tokens[j+1:]), root_offset + tokens[j+1].range[0]) if j < len(tokens) else ""

            # Not a ternary - the type is the type of the first token
            token = tokens[0]
            if token.getKind() == "(": # expression or type cast
                if len(tokens) > 1 and (tokens[1].getKind() in ["w", "(", "{"] or tokens[1].value in ["&", "*"]):  # type cast
                    return self._globals.untypedef(get_base_type_str(token.value[1:-1]))
                    # token_type = self._globals.untypedef(get_base_type_str(token.value[1:-1])) \
                    #              if reg_word_char.match(token.value[1]) else \
                    #              _get_type_of_expr(TokenList(tokens[1:-1]), root_offset + token.range[0] + 1)
                else:
                    token_type = _get_type_of_expr_str(token.value[1:-1], root_offset + token.range[0] + 1)  # expression
            elif reg_word_char.match(token.value):
                token_type = _get_type_of_name(token.value)
            else: # Something weird
                WARNING(_LOC(root_offset + token.range[0]), f"Unexpected token in expression: {token.value}")
                return ""

            # Check for member access chain, return the type of the last member
            i = 1
            while token_type and i < len(tokens):
                if tokens[i].getKind() in ["(", "["]:
                    i += 1
                    if i >= len(tokens):
                        break
                if tokens[i].value not in [".", "->"]:
                    break
                i += 1
                if i >= len(tokens):
                    WARNING(_LOC(root_offset + tokens[-1].range[1]), f"Unexpected end of expression: '{tokens.short_repr()}'")
                    break  # syntax error - stop the chain
                token_type = self._globals.get_field_type(token_type, tokens[i].value)
                i += 1

            return token_type

        chain: AccessChain

        def _get_type_of_expr_str(clean_txt: str, root_offset: int = 0) -> str:
            return self._globals.untypedef(_get_type_of_expr(TokenList(TokenList.xxFilterCode(TokenList.xFromText(clean_txt))), root_offset))

        def _check_access_to_defn(defn2: Definition, offset: int, prefix: str = "") -> None:
            if defn2.is_private and defn2.module and defn2.module != module:
                ERROR(_locationStr(offset), f"Invalid access to private {defn2.kind} '{prefix}{defn2.name}' of [{defn2.module}]")

        def _check_access_to_type(type: str, offset: int) -> None:
            if type in self._globals.types_restricted:
                _check_access_to_defn(self._globals.types_restricted[type], offset)

        def _check_access_to_global_name(name: str, offset: int) -> None:
            DEBUG3(_locationStr(0), f"Access global name: {name}")
            if name in self._globals.names_restricted:
                _check_access_to_defn(self._globals.names_restricted[name], offset)

        def _check_access_to_field(rec_type: str, field: str, offset: int) -> None:
            if rec_type in self._globals.fields and field in self._globals.fields[rec_type]:
                _check_access_to_defn(self._globals.fields[rec_type][field], offset, prefix=f"{rec_type} :: ")

        if invisible_names := self._get_invisible_global_names_for_module(module):
            for match in invisible_names.finditer(body_clean):
                name = match[0]
                ERROR(_locationStr(match.start()), f"Invalid access to private name '{name}' of [{self._globals.names_restricted[name].module}]")

        for chain in member_access_chains(body_clean):
            DEBUG2(_LOC(chain.offset), f"Access chain: {chain}")
            expr_type = _get_type_of_expr_str(chain.name, chain.offset)
            if expr_type:
                DEBUG3(_LOC(chain.offset), f"Access type: {expr_type}")
                _check_access_to_type(expr_type, chain.offset)
                DEBUG3(_LOC(chain.offset), f"Field access chain: {chain}")
                for field in chain.members:
                    DEBUG3(_LOC(chain.offset), f"Field access: {expr_type}->{field}")
                    _check_access_to_field(expr_type, field, chain.offset)
                    if not (expr_type := self._globals.get_field_type(expr_type, field)):
                        WARNING(_LOC(chain.offset), f"Can't deduce type of member '{field}' in {chain}")
                        break  # error
            else:
                WARNING(_locationStr(chain.offset), f"Can't deduce type of expression {chain}")

    # Go through function bodies. Check calls and struct member accesses.
    def checkAccess(self, multithread = False) -> None:
        if not multithread:
            for defn in itertools.chain(
                        self._globals.names.values(),
                        *(namedict.values() for namedict in self._globals.static_names.values())):
                DEBUG3(defn.scope.locationStr(defn.offset), f"debug3: Checking {defn.short_repr()}") or \
                DEBUG(defn.scope.locationStr(defn.offset), f"debug: Checking {defn.kind} [{defn.module}] {defn.name}")
                self._check_function(defn)
        else:
            init_multithreading()
            with multiprocessing.Pool() as pool:
                for res in pool.starmap(
                        AccessCheck._check_function_name_for_multi,
                        itertools.chain(
                            ((self, n, True) for n in self._globals.static_names.keys()),
                            ((self, n, False) for n in self._globals.names.keys()))):
                    print(res, end='')

    @staticmethod
    def _check_function_name_for_multi(self: 'AccessCheck', name: str, file: bool) -> str:
        with LogToStringScope():
            if not file:
                self._check_function(self._globals.names[name])
            else:
                for defn in self._globals.static_names[name].values():
                    self._check_function(defn)
            return workspace.logStream.getvalue() # type: ignore # logStream is a StringIO
