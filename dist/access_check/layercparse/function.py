from typing import Iterable, Any
from dataclasses import dataclass

from .internal import *
from .ctoken import *
from .statement import *
from .variable import *

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
                if tokens[i].getKind() == "(":
                    tokens.pop(i)
            i += 1

        preComment, i = get_pre_comment(tokens)

        # Return type, function name, function args
        retType = TokenList([])
        funcName = None
        argsList = None
        is_const, is_static = False, False
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
                is_const = True
            elif token.value == "static":
                is_static = True
        if funcName is None or argsList is None:
            return None

        retType = TokenList((filter(lambda x: x.value not in c_type_keywords, retType)))

        ret = FunctionParts(retType, funcName, argsList, preComment=preComment, postComment=get_post_comment(tokens), is_type_const=is_const, is_type_static=is_static)

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

    def xGetLocalVars(self) -> Iterable[Variable]:
        if not self.body:
            return
        saved_type: Any = None
        for st in StatementList.xFromText(self.body.value):
            t = st.getKind()
            if saved_type is None and (t.is_statement or (t.is_expression and not t.is_initialization)):
                break
            if saved_type is not None or (t.is_decl and not t.is_function and not t.is_record):
                var = Variable.fromVarDef(st.tokens)
                if var:
                    if not var.typename:
                        var.typename = saved_type
                    yield var
                    saved_type = var.typename if var.end == "," else None
            else:
                saved_type = None
    def getLocalVars(self) -> list[Variable]:
        return list(self.xGetLocalVars())
