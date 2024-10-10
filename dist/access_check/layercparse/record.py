import enum
from dataclasses import dataclass

from . import common
from .ctoken import *
from .statement import *
from .variable import *
from .workspace import *

class RecordKind(enum.Enum):
    UNDEF = 0
    STRUCT = enum.auto()
    UNION = enum.auto()
    ENUM = enum.auto()

@dataclass
class RecordParts:
    recordKind: RecordKind
    name: Token
    typename: TokenList
    body: Token | None = None
    members: list[Variable] | None = None
    typedefs: list[Variable] | None = None
    vardefs: list[Variable] | None = None
    preComment: Token | None = None
    postComment: Token | None = None
    nested: 'list[RecordParts] | None' = None
    parent: 'RecordParts | None' = None
    is_unnamed: bool = False

    def short_repr(self) -> str:
        ret = ["Record(", self.name.value, " : ", repr(self.recordKind)]
        if self.members:
            ret.extend(["".join(["\n  member: "+x.short_repr() for x in self.members])])
        if self.typedefs:
            ret.extend(["".join(["\n  typedef: "+x.short_repr() for x in self.typedefs])])
        if self.vardefs:
            ret.extend(["".join(["\n  vardef: "+x.short_repr() for x in self.vardefs])])
        if self.nested:
            # ret.extend(["".join(["\n  nested: "+x.short_repr() for x in self.nested])])
            ret.extend(["".join(["\n  nested: "+x.name.value for x in self.nested])])
        ret.append(")")
        return "".join(ret)

    def kind(self) -> str:
        return "record"

    def _getBodyOffset(self) -> int:
        ret = 0
        if self.body:
            ret += self.body.range[0]
        if self.parent:
            ret += self.parent._getBodyOffset()
        return ret + scope_offset()

    def update(self, other: 'RecordParts') -> list[str]:
        errors = []
        if self.recordKind != other.recordKind:
            errors.append(f"record type mismatch for '{self.name.value}': {self.recordKind} != {other.recordKind}")
        if self.name != other.name:
            errors.append(f"record name mismatch for '{self.name.value}': {self.name.value} != {other.name.value}")
        # if self.typename != other.typename:
        #     errors.append(f"record name mismatch for {self.typename.value}: {self.typename.value} != {other.typename.value}")
        if self.body is not None and other.body is not None and self.body != other.body:
            errors.append(f"record redifinition: '{self.name.value}'")
        elif other.body is not None:
            self.body = other.body
            self.members = other.members
            self.typedefs = other.typedefs
            self.vardefs = other.vardefs
            self.nested = other.nested
        if self.preComment is None:
            self.preComment = other.preComment
        if self.postComment is None:
            self.postComment = other.postComment
        return errors

    @staticmethod
    def fromStatement(st: Statement, parent: 'RecordParts | None' = None) -> 'RecordParts | None':
        tokens = st.tokens
        ret = RecordParts(RecordKind.UNDEF, Token(-1, (0,0), ""), TokenList([]), parent=parent)

        ret.preComment, i = get_pre_comment(tokens)

        has_names = False
        for i in range(i, len(tokens)):
            token = tokens[i]
            if token.value == "typedef":
                ret.typedefs = []
                has_names = True
            elif token.value == "struct":
                ret.recordKind = RecordKind.STRUCT
            elif token.value == "union":
                ret.recordKind = RecordKind.UNION
            elif token.value == "enum":
                ret.recordKind = RecordKind.ENUM
            elif token.value in c_type_keywords:
                pass
            elif reg_identifier.match(token.value):
                ret.name = token
                ret.typename = TokenList([token])
                has_names = True
            elif token.getKind() == "{":
                ret.body = Token(token.idx, (token.range[0]+1, token.range[1]-1), token.value[1:-1])
                break
            elif token.getKind() == ";":
                return None

        if not ret.body:
            return None

        # vars or types list
        names: list[Variable] = []
        if not ret.name.value:
            ret.name = Token(ret.body.idx, ret.body.range, f"({locationStr(ret._getBodyOffset())})")
            ret.typename = TokenList([ret.name])
        for stt in StatementList.xFromTokens(TokenList(tokens[i+1:])):
            var = Variable.fromVarDef(stt.tokens)
            if var:
                var.typename = ret.typename
                names.append(var)
                has_names = True

        if ret.typedefs is not None:
            ret.typedefs = names
        else:
            ret.vardefs = names

        ret.postComment = get_post_comment(tokens)
        ret.is_unnamed = not has_names

        return ret

    def _xGetMembers(self) -> Iterable[Variable]:
        if not self.body:
            return
        saved_type: Any = None
        var: Variable | None
        for st in StatementList.xFromText(self.body.value):
            t = st.getKind()
            if t.is_preproc:
                continue

            if t.is_record:
                record = RecordParts.fromStatement(st, parent=self)
                if record:
                    if self.nested is None:
                        self.nested = []
                    self.nested.append(record)
                    record.getMembers()
                    if record.vardefs:
                        for var in record.vardefs:
                            yield var
                    elif record.is_unnamed and record.members:  # Pull its members up
                        for var in record.members:
                            yield var
                continue

            var = Variable.fromVarDef(st.tokens)
            if var:
                if not var.typename:
                    var.typename = saved_type
                yield var
                saved_type = var.typename if var.end == "," else None
            else:
                saved_type = None

    def getMembers(self) -> list[Variable]:
        if self.members is None:
            self.members = list(self._xGetMembers())
        return self.members

