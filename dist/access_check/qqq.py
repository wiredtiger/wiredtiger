#!/usr/bin/env python3

import os, sys
from pprint import pprint

from layercparse import *
import wt_defs

# Filter by threads:
# perl -MIO::File -nE '$t = /\[\d++\.\d++\]\[\d++:0x([0-9a-f]++)]/i ? $1 : "-"; if (!$h{$t}) { $h{$t} = IO::File->new(sprintf("q-%03d-%s",++$idx,$t), "w"); } $h{$t}->print($_); sub end() { for (values(%h)) { $_->close(); } exit; } END {end()} BEGIN { $SIG{INT}=\&end; }'

class Patcher:
    txt = ""
    patch_list: list[tuple[tuple[int, int], int, str]]
    idx = 0  # this is used to order the patches for the same range

    def __init__(self):
        self.patch_list = []

    def get_patched(self) -> str:
        ret: list[str] = []
        pos = 0
        for patch in sorted(self.patch_list):
            if patch[0][0] > pos:
                ret.append(self.txt[pos:patch[0][0]])
            ret.append(patch[2])
            pos = patch[0][1]
        ret.append(self.txt[pos:])
        return "".join(ret)

    def _replace(self, range_: tuple[int, int], txt: str) -> None:
        self.idx += 1
        self.patch_list.append((range_, self.idx, txt))

    _session_from_type = {
        "WT_SESSION_IMPL":    lambda arg: f"""{arg}""",
        "WT_SESSION":         lambda arg: f"""(WT_SESSION_IMPL*){arg}""",
        "WT_CURSOR":          lambda arg: f"""CUR2S({arg})""",
        "WT_CONNECTION_IMPL": lambda arg: f"""{arg}->default_session""",
        "WT_CONNECTION":      lambda arg: f"""((WT_CONNECTION_IMPL*){arg})->default_session""",
    }

    @staticmethod
    def _has_session(func: FunctionParts, func_args: list[Variable]) -> str:
        if (not func_args or
                func_args[0].typename[-1].value not in Patcher._session_from_type or
                func.name.value in ["__session_close", "__wt_session_close"] or
                "config_parser_open" in func.name.value):
            return False
        return True

    @staticmethod
    def _get_session(func: FunctionParts, func_args: list[Variable]) -> str:
        return (Patcher._session_from_type[func_args[0].typename[-1].value](func_args[0].name.value)
                if Patcher._has_session(func, func_args)
                else "")

    def patch(self, st: StatementList, func: FunctionParts) -> None:
        if func.typename[-1].value != "int" or "..." in func.args.value:
            return

        func_args = func.getArgs()
        is_api = func.name.value.startswith("wiredtiger_") or "API_" in func.body.value
        if (is_api or
            (func_args and func_args[0].typename[-1].value in [
                "WT_SESSION", "WT_SESSION_IMPL", "WT_CURSOR", "WT_CONNECTION", "WT_CONNECTION_IMPL"])):
            pass # instrument this function
        else:
            return
        if func.name.value.endswith("_pack") or func.name.value in [
                "__block_ext_prealloc", "__block_ext_alloc", "__block_size_alloc",
                "__block_extend", "__block_append", "__block_off_remove", "__block_ext_insert",
                "__block_extlist_dump",
                "__wt_compare"]:
            return

        session = Patcher._get_session(func, func_args)

        is_api_str = " API" if is_api else ""
        static = "static " if func.is_type_static else ""

        # Insert a forward declaration of the wrapper function for the case of recursive calls
        self._replace((st.range()[0], st.range()[0]), f"""
{static}{func.typename.short_repr()} {func.name.value}({func.args.value});
""")

        # Add "static" to the original
        if not func.is_type_static:
            self._replace((func.typename[0].range[0], func.typename[0].range[0]), f"static ")
        # Rename the original function
        self._replace(func.name.range, f"{func.name.value}__orig_")

        # Create a new function with the original name

        has_ret = func.typename[-1].value == "int"

        # /* {[arg.typename[0].value for arg in func_args]} */
        # /* {[arg.typename[-1].value for arg in func_args]} */
        # /* {[arg.name.value for arg in func_args]} */
        self._replace((st.range()[1], st.range()[1]), f"""
{static}{func.typename.short_repr()}
{func.name.value}({func.args.value}) {{
    __WT_CALL_WRAP{"" if has_ret else "_NORET"}(
        "{func.name.value}{is_api_str}",
        {func.name.value}__orig_({", ".join((v.name.value for v in func_args))}),
        {session or "NULL"});
}}
""")

    def parseDetailsFromText(self, txt: str, offset: int = 0) -> None:
        with ScopePush(offset=offset):
            self.txt = txt
            for st in StatementList.fromText(txt, 0):
                st.getKind()
                if st.getKind().is_function_def:
                    func = FunctionParts.fromStatement(st)
                    if func:
                        # pprint(func, width=120)
                        # pprint(func.getArgs(), width=120)
                        self.patch(st, func)

    def parseDetailsFromFile(self, fname: str) -> None:
        with ScopePush(file=fname):
            return self.parseDetailsFromText(scope_file().read())

def main():
    rootPath = os.path.realpath(sys.argv[1])
    setRootPath(rootPath)
    setModules(wt_defs.modules)

    files = get_files()  # list of all source files

    for file in files:
        if not (mod := fname_to_module(file)):
            continue
        print(f" === [{mod}] {file}")

        parcher = Patcher()
        parcher.parseDetailsFromFile(file)
        # print(parcher.get_patched())
        # write file back
        with open(file, "w") as f:
            f.write(parcher.get_patched())

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except OSError as e:
        print(f"\n{e.strerror}: {e.filename}")
        sys.exit(1)

