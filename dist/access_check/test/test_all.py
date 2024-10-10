#!/usr/bin/env python3

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.dirname(__file__))

import unittest
from unittest.util import _common_shorten_repr
from copy import deepcopy

from layercparse import *
from pprint import pprint, pformat
from io import StringIO

import difflib


def pf(obj: Any) -> str:
    return pformat(obj, width=120, compact=False)


class TestCaseLocal(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.maxDiff = None

    def assertMultiLineEqualDiff(self, result, expected, msg=None):
        """Assert that two multi-line strings are equal."""
        self.assertIsInstance(result, str, 'First argument is not a string')
        self.assertIsInstance(expected, str, 'Second argument is not a string')

        if result.rstrip() != expected.rstrip():
            resultlines = result.splitlines(keepends=False)
            expectedlines = expected.splitlines(keepends=False)
            if len(resultlines) == 1 and result.strip('\r\n') == result:
                resultlines = [result + '\n']
                expectedlines = [expected + '\n']
            standardMsg = '%s != %s' % _common_shorten_repr(result, expected)
            diff = '\n' + '\n'.join(difflib.unified_diff(expectedlines, resultlines))
            standardMsg = self._truncateMessage(standardMsg, diff)
            self.fail(self._formatMessage(msg, standardMsg))

    def checkStrAgainstFile(self, result, fname):
        result = regex.sub(r" with id=\d++", " with id=*", result, re_flags)
        with open(f"{fname}.test", "w") as f:
            f.write(result)
        self.assertMultiLineEqualDiff(result, file_content(fname))

    def checkObjAgainstFile(self, result, fname):
        self.checkStrAgainstFile(pf(result), fname)

    def parseDetailsFromText(self, txt: str, offset: int = 0) -> str:
        a = []
        with ScopePush(offset=offset):
            for st in StatementList.fromText(txt, 0):
                st.getKind()
                a.append(pf(st))
                # a.append(pf(StatementKind.fromTokens(st.tokens)))
                if st.getKind().is_function_def:
                    func = FunctionParts.fromStatement(st)
                    self.assertIsNotNone(func)
                    if func:
                        a.extend(["Function:", pf(func)])
                        a.extend(["Args:", pf(func.getArgs())])
                        if func.body:
                            a.extend(["Vars:", pf(func.getLocalVars())])
                elif st.getKind().is_record:
                    record = RecordParts.fromStatement(st)
                    self.assertIsNotNone(record)
                    if record:
                        members = record.getMembers()
                        a.extend(["Record:", pf(record)])
                        # a.extend(["Members:", pf(members)])
                elif st.getKind().is_extern_c:
                    body = next((t for t in st.tokens if t.value[0] == "{"), None)
                    self.assertIsNotNone(body)
                    if body:
                        a.append(self.parseDetailsFromText(body.value[1:-1], offset=body.range[0]+1))
                elif st.getKind().is_decl and not st.getKind().is_function and not st.getKind().is_record:
                    var = Variable.fromVarDef(st.tokens)
                    if var:
                        a.extend(["Variable:", pf(var)])
                elif st.getKind().is_preproc:
                    macro = MacroParts.fromStatement(st)
                    if macro:
                        a.extend(["Macro:", pf(macro)])
        return "\n".join(a)

    def parseDetailsFromFile(self, fname: str) -> str:
        with ScopePush(file=fname):
            return self.parseDetailsFromText(file_content(fname))


class TestRegex(TestCaseLocal):
    def test_regex(self):
        self.assertListEqual(reg_token.match("qwe").captures(),
            ["qwe"])
        self.assertListEqual(reg_token.findall("qwe"),
            ["qwe"])
        self.assertListEqual(reg_token.findall("qwe\\\nasd"),
            ['qwe', '\\\n', 'asd'])
        self.assertListEqual(reg_token.findall("qwe(asd)  {zxc} \n [wer]"),
            ["qwe", "(asd)", "  ", "{zxc}", " ", "\n", " ", "[wer]"])
        self.assertListEqual(reg_token.findall(r"""/* qwe(asd*/ "as\"d" {zxc} """+"\n [wer]"),
            ['/* qwe(asd*/', ' ', '"as\\"d"', ' ', '{zxc}', ' ', '\n', ' ', '[wer]'])
        self.assertListEqual(reg_token.findall(r"""/* qwe(asd*/ "as\"d" {z/*xc} """+"\n [wer]*/}"),
            ['/* qwe(asd*/', ' ', '"as\\"d"', ' ', '{z/*xc} \n [wer]*/}'])
        self.assertListEqual(reg_token.findall(r"""int main(int argc, char *argv[]) {\n  int a = 1;\n  return a;\n}"""),
            ['int', ' ', 'main', '(int argc, char *argv[])', ' ', '{\\n  int a = 1;\\n  return a;\\n}'])

    def test_regex_r(self):
        self.assertListEqual(reg_token_r.match("qwe").captures(),
            ["qwe"])
        self.assertListEqual(reg_token_r.findall("qwe"),
            ["qwe"])
        # self.assertListEqual(reg_token_r.findall("qwe\\\nasd"),
        #     list(reversed(['qwe', '\\\n', 'asd'])))
        self.assertListEqual(reg_token_r.findall("qwe(asd)  {zxc} \n [wer]"),
            list(reversed(["qwe", "(asd)", "  ", "{zxc}", " ", "\n", " ", "[wer]"])))
        self.assertListEqual(reg_token_r.findall(r"""/* qwe(asd*/ "as\"d" {zxc} """+"\n [wer]"),
            list(reversed(['/* qwe(asd*/', ' ', '"as\\"d"', ' ', '{zxc}', ' ', '\n', ' ', '[wer]'])))
        self.assertListEqual(reg_token_r.findall(r"""/* qwe(asd*/ "as\"d" {z/*xc} """+"\n [wer]*/}"),
            list(reversed(['/* qwe(asd*/', ' ', '"as\\"d"', ' ', '{z/*xc} \n [wer]*/}'])))
        self.assertListEqual(reg_token_r.findall(r"""int main(int argc, char *argv[]) {\n  int a = 1;\n  return a;\n}"""),
            list(reversed(['int', ' ', 'main', '(int argc, char *argv[])', ' ', '{\\n  int a = 1;\\n  return a;\\n}'])))

    def test_clean(self):
        self.assertEqual(clean_text_sz("qwe asd /* zxc\n */ wer"), "qwe asd       \n    wer")
        self.assertEqual(clean_text("qwe asd /* zxc\n */ wer"), "qwe asd   wer")
        self.assertEqual(clean_text_compact("qwe asd /* zxc\n */ wer"), "qwe asd wer")
        self.assertEqual(clean_text_sz("qwe 'QQQ  /* WWW */ ' asd /* zxc\n */ wer"), "qwe 'QQQ  /* WWW */ ' asd       \n    wer")
        self.assertEqual(clean_text("qwe 'QQQ  /* WWW */ ' asd /* zxc\n */ wer"), "qwe 'QQQ  /* WWW */ ' asd   wer")
        self.assertEqual(clean_text_compact("qwe 'QQQ  /* WWW */ ' asd /* zxc\n */ wer"), "qwe 'QQQ  /* WWW */ ' asd wer")


class TestToken(TestCaseLocal):
    def test_token(self):
        self.checkObjAgainstFile(TokenList.fromFile("data/block.h"), "data/block.h.tokens")


class TestVariable(TestCaseLocal):
    def test_1(self):
        self.assertMultiLineEqualDiff(repr(Variable.fromVarDef(TokenList.fromText("int a;", 0))),
            r"""Variable(name=Token(idx=2, range=(4, 5), value='a'), typename=[0:3] 〈int〉, preComment=None, postComment=None, end=';')""")
    def test_2(self):
        self.assertMultiLineEqualDiff(repr(Variable.fromVarDef(TokenList.fromText("int (*a)(void);", 0))),
            r"""Variable(name=Token(idx=2, range=(4, 8), value='a'), typename=[0:3] 〈int〉, preComment=None, postComment=None, end=';')""")
    def test_3(self):
        self.assertMultiLineEqualDiff(repr(Variable.fromVarDef(TokenList.fromText("int a[10];", 0))),
            r"""Variable(name=Token(idx=2, range=(4, 5), value='a'), typename=[0:3] 〈int〉, preComment=None, postComment=None, end=';')""")
    def test_4(self):
        self.assertMultiLineEqualDiff(repr(Variable.fromVarDef(TokenList.fromText("int *a[10];", 0))),
            r"""Variable(name=Token(idx=3, range=(5, 6), value='a'), typename=[0:3] 〈int〉, preComment=None, postComment=None, end=';')""")


class TestStatement(TestCaseLocal):
    def test_statement(self):
        with ScopePush(file=File("data/block.h")):
            self.checkObjAgainstFile(StatementList.fromFile("data/block.h"), "data/block.h.statements")

    def test_statement_details(self):
        self.checkStrAgainstFile(self.parseDetailsFromFile("data/block.h"), "data/block.h.statements-details")


class TestStatementDetails(TestCaseLocal):
    def test_func(self):
        self.checkStrAgainstFile(self.parseDetailsFromFile("data/func_simple.c"), "data/func_simple.c.statements-details")

    def test_various(self):
        self.checkStrAgainstFile(self.parseDetailsFromFile("data/various.c"), "data/various.c.statements-details")

    def test_statement_types(self):
        self.checkStrAgainstFile(self.parseDetailsFromFile("data/statements.c"), "data/statements.c.statements-details")


class TestRecordAccess(TestCaseLocal):
    def test_record(self):
        workspace.logStream = StringIO()
        setModules([Module("module1"), Module("module2")])
        setLogLevel(LogLevel.DEBUG3)
        _globals = Codebase()
        _globals.updateFromFile("data/record.c", expand_preproc=False)
        # print(" ===== Globals:")
        # pprint(_globals, width=120, compact=False)
        # print(" =====")
        # setLogLevel(LogLevel.DEBUG)
        AccessCheck(_globals).checkAccess(multithread=False)
        self.checkStrAgainstFile(workspace.logStream.getvalue(), "data/record.c.access")
        workspace.logStream = None
        setLogLevel(LogLevel.DEFAULT)


class TestMacro(TestCaseLocal):
    def test_macro(self):
        _globals = Codebase()
        src = file_content("data/macro.c")
        for p in StatementList.preprocFromText(src):
            _globals.addMacro(MacroParts.fromStatement(p))

        # _globals.updateFromFile("data/macro.c")
        # setLogLevel(LogLevel.DEBUG)
        # pprint(_globals.macros, width=120, compact=False)

        expanded = MacroExpander().expand(src, _globals.macros___, expand_const=True)
        self.checkStrAgainstFile(expanded, "data/macro.c.macro-full")

        expanded = MacroExpander().expand(src, _globals.macros___, expand_const=False)
        self.checkStrAgainstFile(expanded, "data/macro.c.macro-noconst")

    def test_macro_expand(self):
        setModules([Module("mod1"), Module("mod2")])

        workspace.logStream = StringIO()
        _globals1 = Codebase()
        _globals1.scanFiles(["data/macro-expand-offsets.c"], twopass=True, multithread=False)
        AccessCheck(_globals1).checkAccess(multithread=False)
        check1 = workspace.logStream.getvalue()

        workspace.logStream = StringIO()
        _globals2 = Codebase()
        _globals2.scanFiles(["data/macro-expand-offsets.c"], twopass=False, multithread=False)
        AccessCheck(_globals2).checkAccess(multithread=False)
        check2 = workspace.logStream.getvalue()

        self.assertTrue(check1.find("Invalid access") != -1)
        self.assertMultiLineEqualDiff(check1, check2)

class TestCodebase(TestCaseLocal):
    def test_codebase(self):
        _globals = Codebase()
        _globals.scanFiles(["data/statements.c"], twopass=False, multithread=False)
        self.checkStrAgainstFile(pformat(_globals, width=120, compact=False), "data/statements.c.globals")


# Enable to run as a standalone script
if __name__ == "__main__":
    unittest.TextTestRunner().run(unittest.TestLoader().discover(os.path.dirname(__file__)))
