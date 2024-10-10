import enum
from itertools import islice
from dataclasses import dataclass
from typing import Iterable

from .ctoken import *

@dataclass
class StatementKind:
    is_comment: bool | None = None
    is_preproc: bool | None = None
    is_typedef: bool | None = None
    is_record: bool | None = None
    is_function: bool | None = None
    is_function_def: bool | None = None
    is_function_decl: bool | None = None
    is_statement: bool | None = None
    is_decl: bool | None = None
    is_expression: bool | None = None
    is_initialization: bool | None = None
    is_extern_c: bool | None = None
    is_unnamed_record: bool | None = None # unnamed struct/union/enum which pulls its members into the parent scope
    end: str | None = None
    preComment: Token | None = None
    postComment: Token | None = None

    @staticmethod
    def fromTokens(tokens: TokenList) -> 'StatementKind':
        ret = StatementKind()
        if not tokens:
            return ret
        for token in tokens:
            if token.getKind() == " ":
                continue
            if token.getKind() == "/":
                ret.is_comment = True
                ret.preComment = token
                continue
            if token.getKind() == "#":
                ret.is_preproc = True
                return ret
            if token.value in c_statement_keywords:
                ret.is_statement = True
                return ret
            if token.getKind() not in [" ", "#", "/"]:
                break
        else:
            # we get here if "break" was not executed
            return ret

        # Only get here if we have a non-empty token
        clean_tokens = tokens.filterCode()

        i = 0
        while i < len(clean_tokens):
            if clean_tokens[i].value in ignore_type_keywords:
                clean_tokens.pop(i)
                if clean_tokens[i].getKind() == "(":
                    clean_tokens.pop(i)
            elif clean_tokens[i].value in ["const", "static"]:
                clean_tokens.pop(i)
            else:
                i += 1

        if not clean_tokens:
            return ret

        if len(clean_tokens) == 1:
            ret.is_expression = True
            return ret

        # From here the options are:
        # - typedef
        # - record
        # - function
        # - expression
        # - declaration
        # - declaration + initialization (expression)

        ret.postComment = get_post_comment(tokens)

        if clean_tokens[0].value == "extern":
            if len(clean_tokens) > 1 and clean_tokens[1].value == '"C"':
                ret.is_extern_c = True
            # Ignore any type of "extern" declaration - rely on the actual one
            return ret

        if clean_tokens[0].value == "typedef":
            ret.is_typedef = True
            clean_tokens.pop(0)
            if not clean_tokens:
                return ret

        if not ret.is_typedef:
            # Filter tokens relevant to declaration. Take first two elements
            tokens_decl = list(islice(filter(lambda t: (t.getKind() == "{" or
                                                        (t.getKind() == "+" and t.value != "*") or
                                                        (t.getKind() == "w" and t.value not in ["struct", "union", "enum", "*"])),
                                clean_tokens),
                            0, 2))
            if len(tokens_decl) < 2:
                # ret.is_expression = True
                if len(clean_tokens) == 2 and clean_tokens[0].value in ["struct", "union"] and clean_tokens[1].getKind() == "{":
                    ret.is_record = True
                    ret.is_unnamed_record = True
                return ret
            if ((tokens_decl[0].getKind() == "w" and tokens_decl[1].getKind() == "w") or
                    tokens_decl[0].value in c_type_keywords or
                    tokens_decl[0].value in c_types):
                ret.is_decl = True

            for i in range(1, len(clean_tokens)-1):
                token = clean_tokens[i]
                if token.value == "=":
                    ret.is_expression = True
                    if ret.is_decl or clean_tokens[0].value in ["struct", "union", "enum"]:
                        ret.is_initialization = True
                    break
                elif token.getKind() == "+":
                    if token.value == "*": # and clean_tokens[i+1].idx - token.idx == 1:
                        pass # pointer dereference
                    else:
                        ret.is_expression = True
                        break

        # There is a curly brace in the tokens (before the = if there is one)
        curly = next((token.getKind() == "{" for token in clean_tokens if token.getKind() == "{" or token.value == "="), False)

        if clean_tokens[0].value in ["struct", "union", "enum"]:
            if curly:
                ret.is_record = True
            else:
                if not ret.is_typedef:
                    ret.is_decl = True
            return ret

        if ret.is_typedef:
            return ret

        # Not a typedef or record

        # There are at least two tokens

        for i in range(1, len(clean_tokens)):
            token = clean_tokens[i]
            if token.value.startswith("("):
                if reg_identifier.match(clean_tokens[i-1].value):   # word followed by (
                    ret.is_function = True
                    if ret.is_decl:
                        ret.is_function_decl = True
                        if curly:                                   # has a body
                            ret.is_function_def = True
                break

        return ret


@dataclass
class Statement:
    tokens: TokenList
    kind: StatementKind | None = None

    def range(self) -> Range:
        return self.tokens.range()

    def xFilterCode(self) -> Iterable[Token]:
        return self.tokens.xFilterCode()
    def filterCode(self) -> 'Statement':
        return Statement(self.tokens.filterCode(), self.kind)

    def xFilterCode_r(self) -> Iterable[Token]:
        return self.tokens.xFilterCode_r()
    def filterCode_r(self) -> 'Statement':
        return Statement(self.tokens.filterCode_r(), self.kind)

    def getKind(self) -> StatementKind:
        if not self.kind:
            self.kind = StatementKind.fromTokens(self.tokens)
        return self.kind


_reg_preproc_only = regex.compile(r"""
    (?> (?: \# | \/\/ ) (?: [^\\\n] | \\. )*+ \n) |
    (?> \/\* (?: [^*] | \*[^\/] )*+ \*\/ ) |
    (?> " (?> [^\\"] | \\. )* " ) |
    (?> ' (?> [^\\'] | \\. )* ' ) |
    (?>\s++) |
    (?>[^\/\#"']++) |
    .
""", re_flags)


# class StatementList: ...
class StatementList(list[Statement]):
    def range(self):
        return self[0].range[0], self[-1].range[1] if len(self) > 0 else (0, 0)

    @staticmethod
    def xFromFile(fname: str, **kwargs) -> Iterable[Statement]:
        return StatementList.xFromTokens(TokenList.fromFile(fname, **kwargs))
    @staticmethod
    def fromFile(fname: str, **kwargs) -> 'StatementList':
        return StatementList.fromTokens(TokenList.fromFile(fname, **kwargs))

    @staticmethod
    def xFromText(txt: str, **kwargs) -> Iterable[Statement]:
        return StatementList.xFromTokens(TokenList.fromText(txt, **kwargs))
    @staticmethod
    def fromText(txt: str, **kwargs) -> 'StatementList':
        return StatementList.fromTokens(TokenList.fromText(txt, **kwargs))

    @staticmethod
    def xFromTokens(tokens: TokenList) -> Iterable[Statement]:
        cur, complete, statement_special, curly, comment_only, is_record, is_expr = TokenList([]), False, 0, False, None, False, False
        else_idx = -1

        def push_statement():
            nonlocal cur, complete, statement_special, curly, comment_only, is_record, is_expr
            ret = Statement(cur)
            cur, complete, statement_special, curly, comment_only, is_record, is_expr = TokenList([]), False, 0, False, None, False, False
            return ret

        for i in range(len(tokens)):
            def find_else():
                nonlocal else_idx, i
                if else_idx > i:
                    return tokens[else_idx].value == "else"
                for ii in range(i+1, len(tokens)):
                    if tokens[ii].value == "else":
                        else_idx = ii
                        return True
                    if tokens[ii].value.startswith(";") or tokens[ii].getKind() not in [" ", "#", "/"]:
                        else_idx = ii
                        return False

            token = tokens[i]

            if token.getKind() == "@":  # invalid token
                if cur:
                    yield push_statement()
                yield Statement(TokenList([token]))

            if (complete and token.getKind() not in [" ", "/"]) or \
               (comment_only and token.getKind() == "/"):
                    yield push_statement()

            if comment_only is None and token.getKind() == "/":
                comment_only = True
            elif comment_only is not False and token.getKind() not in [" ", "/"]:
                comment_only = False
            if not is_expr and token.getKind() == "+" and token.value != "*":
                is_expr = True

            # print(f"i={i}, stype={stype}, token=<{token.value}>, is_thing={is_thing}, is_word={is_word}, is_type={is_type}")

            if not statement_special:   # Constructs that don't end by ; or {}
                if token.value == "if": # if can continue with else after ;
                    statement_special = 1
                elif token.value in ["struct", "union", "enum", "typedef"]:  # These end strictly with a ;
                    statement_special = 2
                    is_record = True
                elif is_expr or token.value == "do":  # These end strictly with a ;
                    statement_special = 2

            cur.append(token)

            if (complete and token.value == "\n") or token.getKind() == "#": # preproc is always a single token
                yield push_statement()
                continue

            if token.value[0] not in [";", ",", "{"]:  # Any statement ends with one of these
                continue

            if token.value.startswith("{"):
                curly = True
            elif token.value in [";", ","] and statement_special == 2:
                if is_record and not curly:
                    is_record = False
                    statement_special = 0

            if statement_special == 1:
                if find_else():
                    continue
            elif statement_special == 2 and token.value[0] != ";":
                continue

            complete = True  # The statement is complete but may want to attach trailing \n or comment

        if cur:
            yield push_statement()
    @staticmethod
    def fromTokens(tokens: TokenList) -> 'StatementList':
        return StatementList(StatementList.xFromTokens(tokens))

    def xFilterCode(self) -> Iterable[Statement]:
        for st in self:
            yield st.filterCode()
    def filterCode(self) -> 'StatementList':
        return StatementList(self.xFilterCode())

    def xFilterCode_r(self) -> Iterable[Statement]:
        for st in self:
            yield st.filterCode_r()
    def filterCode_r(self) -> 'StatementList':
        return StatementList(self.xFilterCode_r())

    @staticmethod
    def preprocFromText(txt: str) -> Iterable[Statement]:
        prev: list[Token] = [Token.empty(), Token.empty()]  # previous 2 tokens
        cur_prev = 0
        i = 0
        ret = TokenList([])
        for match in _reg_preproc_only.finditer(txt):
            i += 1
            if match[0].startswith("#"):
                token = Token.fromMatch(match, kind="#", idx=i)
                prev_tokens = [prev[1-cur_prev], prev[cur_prev]]
                if prev_tokens[0].getKind() == "/" and prev_tokens[1].getKind() == " ":
                    yield Statement(TokenList([prev_tokens[0], prev_tokens[1], token]), StatementKind(is_comment=True, is_preproc=True))
                else:
                    yield Statement(TokenList([token]), StatementKind(is_preproc=True))
            cur_prev = 1 - cur_prev
            prev[cur_prev] = Token.fromMatch(match, idx=i)

    @staticmethod
    def preprocFromFile(fname: str) -> Iterable[Statement]:
        with open(fname) as file:
            return StatementList.preprocFromText(file.read())

