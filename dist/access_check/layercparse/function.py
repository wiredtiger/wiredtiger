from typing import Iterable, Any
from dataclasses import dataclass

from .internal import *
from .ctoken import *
from .statement import *
from .variable import *
from .record import *

@dataclass
class FunctionParts:
    typename: TokenList
    name: Token
    args: Token
    body: Token | None = None
    preComment: Token | None = None
    postComment: Token | None = None
    is_type_const: bool = False
    is_type_static: bool = False

    def short_repr(self) -> str:
        return f"Function({self.name} ({self.args})) : {self.typename}"

    def kind(self) -> str:
        return "function"

    def update(self, other: 'FunctionParts') -> list[str]:
        errors = []
        if self.typename != other.typename:
            errors.append(f"function retType mismatch for '{self.name.value}': '{self.typename.short_repr()}' != '{other.typename.short_repr()}'")
        if self.name != other.name:
            errors.append(f"function name mismatch for '{self.name.value}': '{self.name.value}' != '{other.name.value}'")
        if self.args != other.args:
            errors.append(f"function args mismatch for '{self.name.value}': '{self.args.value}' != '{other.args.value}'")
        if self.body is not None and other.body is not None and self.body != other.body:
            errors.append(f"function redifinition: '{self.name.value}'")
        if self.preComment is None:
            self.preComment = other.preComment
        if self.postComment is None:
            self.postComment = other.postComment
        return errors

    @staticmethod
    def fromStatement(statement: Statement) -> 'FunctionParts | None':
        tokens = TokenList([t for t in statement.tokens]) # Shallow copy

        i = 0
        while i < len(tokens):
            if tokens[i].value in ignore_type_keywords:
                tokens.pop(i)
                if i < len(tokens) and tokens[i].getKind() == "(":
                    tokens.pop(i)
            i += 1

        preComment, i = get_pre_comment(tokens)

        # Return type, function name, function args
        retType = TokenList([])
        funcName = None
        argsList = None
        is_type_const, is_type_static = False, False
        for i in range(i, len(tokens)):
            token = tokens[i]
            if token.getKind() == "(":
                if retType:
                    funcName = retType.pop()
                argsList = Token(token.idx, (token.range[0]+1, token.range[1]-1), token.value[1:-1])
                break
            if token.getKind() not in [" ", "#", "/"]:
                retType.append(token)
            if token.value == "const":
                is_type_const = True
            elif token.value == "static":
                is_type_static = True
        if funcName is None or argsList is None:
            return None

        retType = TokenList((filter(lambda x: x.value not in c_type_keywords, retType)))

        ret = FunctionParts(retType, funcName, argsList, preComment=preComment, postComment=get_post_comment(tokens), is_type_const=is_type_const, is_type_static=is_type_static)

        # Function body
        for i in range(i+1, len(tokens)):
            token = tokens[i]
            if token.value[0] == "{":
                ret.body = Token(token.idx, (token.range[0]+1, token.range[1]-1), token.value[1:-1])
                break

        return ret

    def xGetArgs(self) -> Iterable[Variable]:
        for stt in StatementList.xFromText(self.args.value):
            var = Variable.fromVarDef(stt.tokens)
            if var:
                yield var
    def getArgs(self) -> list[Variable]:
        return list(self.xGetArgs())

    def xGetLocalVars(self, _globals: 'Codebase | None' = None) -> Iterable[Variable]: # type: ignore[name-defined] # error: Name "Codebase" is not defined (circular dependency)
        if not self.body:
            return
        saved_type: Any = None
        for st in StatementList.xFromText(self.body.value):
            t = st.getKind()
            if not saved_type and not t.is_decl and (t.is_statement or (t.is_expression and not t.is_initialization)):
                break
            # TODO: Add local variables from where t.is_record
            if saved_type or (t.is_decl and not t.is_function and not t.is_record):
                var = Variable.fromVarDef(st.tokens)
                if var:
                    if not var.typename:
                        var.typename = saved_type
                    yield var
                    saved_type = var.typename if var.end == "," else None
            else:
                saved_type = None
                if t.is_record:
                    with ScopePush(offset=self.body.range[0]):
                        record = RecordParts.fromStatement(st)
                    if record:
                        if _globals:
                            _globals.addRecord(record, is_global_scope=False)
                        if record.vardefs:
                            yield from record.vardefs

    def getLocalVars(self, _globals: 'Codebase | None' = None) -> list[Variable]: # type: ignore[name-defined] # error: Name "Codebase" is not defined (circular dependency)
        return list(self.xGetLocalVars(_globals))
