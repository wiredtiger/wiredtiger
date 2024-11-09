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

    def replace(self, range_: tuple[int, int], txt: str) -> None:
        self.idx += 1
        self.patch_list.append((range_, self.idx, txt))

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

        session_getter = {
            "WT_SESSION_IMPL": lambda arg: f"""{arg}""",
            "WT_SESSION": lambda arg: f"""(WT_SESSION_IMPL*){arg}""",
            "WT_CURSOR": lambda arg: f"""CUR2S({arg})""",
            "WT_CONNECTION_IMPL": lambda arg: f"""{arg}->default_session""",
            "WT_CONNECTION": lambda arg: f"""((WT_CONNECTION_IMPL*){arg})->default_session""",
        }
        session = (
            session_getter[func_args[0].typename[-1].value](func_args[0].name.value)
            if (func_args and
                func_args[0].typename[-1].value in session_getter and
                func.name.value not in ["__session_close", "__wt_session_close"] and
                "config_parser_open" not in func.name.value) else
            "")

        enter_session = f"""    WT_SESSION_IMPL *__session__ = {session};\n""" if session else ""

        set_indent = (lambda ts: f"""    __WT_SET_INDENT();
    __WT_SET_SESSION_INFO({ts});
""") if session else (lambda ts: f"""    __WT_SET_INDENT();
    __WT_SET_NOSESSION_INFO({ts});
""")

        session_info = 'wt_calltrack._session_info_buf' if session else 'wt_calltrack._session_info_buf'

        printf_preambula = "__wt_errx(__session__, " if session else "printf("
        escape = ((lambda s: s.replace('"', '\\"').replace("\n", "\\n").replace("\t", "\\t"))
                  if session else
                  (lambda s: s.replace('"', '\\"').replace("\n", "\\n").replace("\t", "\\t") + "\\n"))
        printf = lambda fmt, *args: f"""{printf_preambula}"{escape(fmt)}", {", ".join(args)});"""
        is_api_str = " API" if is_api else ""
        static = "static " if func.is_type_static else ""

        # Insert a forward declaration of the wrapper function for the case of recursive calls
        self.replace((st.range()[0], st.range()[0]), f"""
{static}{func.typename.short_repr()} {func.name.value}({func.args.value});
""")

        # Add "static" to the original
        if not func.is_type_static:
            self.replace((func.typename[0].range[0], func.typename[0].range[0]), f"static ")
        # Rename the original function
        self.replace(func.name.range, f"{func.name.value}__orig_")

        # Create a new function with the original name

        report_enter = f"""    printf("%3d%s%s ...                       \\t\\t%s: %s:%d: %s\\n",
        wt_calltrack.nest_level, wt_calltrack._indent_buf, "{func.name.value}{is_api_str}",
        {session_info}, __FILE__, __LINE__, __PRETTY_FUNCTION__);"""
        if func.typename[-1].value != "int":
            ret_set = "    "
            # report_ret = ""
            report_ret = f"""    printf("%3d%s%s  (" PRtimespec " / " PRtimespec ")\\t\\t%s: %s:%d: %s\\n",
        wt_calltrack.nest_level, wt_calltrack._indent_buf, "{func.name.value}{is_api_str}",
        PRtimespec_arg(__tt_diff__), PRtimespec_arg(__ts_diff__),
        {session_info}, __FILE__, __LINE__, __PRETTY_FUNCTION__);"""
            ret_ret = ""
        else:
            ret_set = f"    {func.typename.short_repr()} __ret__ = "
            # report_ret = ""
            report_ret = f"""    printf("%3d%s%s = %d  (" PRtimespec " / " PRtimespec ")\\t\\t%s: %s:%d: %s\\n",
        wt_calltrack.nest_level, wt_calltrack._indent_buf, "{func.name.value}{is_api_str}", __ret__,
        PRtimespec_arg(__tt_diff__), PRtimespec_arg(__ts_diff__),
        {session_info}, __FILE__, __LINE__, __PRETTY_FUNCTION__);"""
            ret_ret = "    return __ret__;"

        # /* {[arg.typename[0].value for arg in func_args]} */
        # /* {[arg.typename[-1].value for arg in func_args]} */
        # /* {[arg.name.value for arg in func_args]} */
        self.replace((st.range()[1], st.range()[1]), f"""
{static}{func.typename.short_repr()}
{func.name.value}({func.args.value}) {{
    ++wt_calltrack.nest_level;
    struct timespec __ts_start__, __ts_end__, __ts_diff__;
    struct timespec __tt_start__, __tt_end__, __tt_diff__;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &__tt_start__);
    __wt_epoch_raw(NULL, &__ts_start__);
{enter_session}{set_indent("__ts_start__")}{report_enter}
{ret_set}{func.name.value}__orig_({", ".join((v.name.value for v in func_args))});
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &__tt_end__);
    __wt_epoch_raw(NULL, &__ts_end__);
    __wt_timespec_diff(&__ts_start__, &__ts_end__, &__ts_diff__);
    __wt_timespec_diff(&__tt_start__, &__tt_end__, &__tt_diff__);
{set_indent("__ts_end__")}{report_ret}
    --wt_calltrack.nest_level;
{ret_ret}
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

