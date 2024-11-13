#!/usr/bin/env python3

import os, sys
from pprint import pprint

from layercparse import *
import wt_defs

# Filter by threads:
# cd .; rm -rf WT_TEST q-* ; time ./wtperf -O ~/tmp/mongodb-oplog.wtperf 2>&1 | pv | perl -MIO::File -nE '$t = /\t\[\d++:0x([0-9a-f]++)]/i ? $1 : "-"; if (!$h{$t}) { $h{$t} = IO::File->new(sprintf("q-%03d-%s",++$idx,$t), "w"); } $h{$t}->print($_); sub end() { for (values(%h)) { $_->close(); } exit; } END {end()} BEGIN { $SIG{INT}=\&end; }'

# Graphical view:
# cd .; rm -rf WT_TEST q-* ; time ./wtperf -O ~/tmp/mongodb-oplog.wtperf 2>&1 | head -5000000| pv > q.json ; echo "{}]}" >> q.json

# By threads:
# cd .; rm -rf WT_TEST q-* ; time ./wtperf -O ~/tmp/mongodb-oplog.wtperf 2>&1 | pv | perl -MIO::File -nE 'next if !/"tid": (\d++)/i; $t=$1; if (!$h{$t}) { $h{$t} = IO::File->new(sprintf("q-%03d-%s.json",++$idx,$t), "w"); $h{$t}->say(q/{"traceEvents": [/) } $h{$t}->print($_); sub end() { for (values(%h)) { $_->say(q/{}]}/); $_->close(); } exit; } END {end()} BEGIN { $SIG{INT}=\&end; }'
# Arrange by servers:
# SERVERS=$(for f in q-[0-9]*; do head -10 $f | grep -oE '"[a-zA-Z0-9_]*_(server|run)[":]'; done | tr -d '":' | sort -u); for SERVER in $SERVERS; do FILES=$(for f in q-[0-9]*; do head -10 $f | fgrep -q '"'$SERVER && echo $f; done); echo $SERVER " : " $FILES; perl -nE 'BEGIN { say q/{"traceEvents": [/ } END { say q/{}]}/ } print if /"tid":/' $FILES > q-$SERVER.json; done

# Check what threads are doing:
# for f in calltrack-[0-9]*; do echo "$f " $(head -10 $f | grep -oE '"[a-zA-Z0-9_]*_(server|run)[":]'); done | tr -d '":'

# Merge some threads:
#  Plain
# perl -MIO::File -E 'sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && $l[0][0] <= $l[1][0])) { print $l[0][1]; $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { say q/{"traceEvents": [/ } END { say q/{}]}/ }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..45}.json | pv > q.json
#  Split by 12M lines
# perl -MIO::File -E 'sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && $l[0][0] <= $l[1][0])) { print F $l[0][1]; if (++$num > 12e6) { $num=0; say F q/{}]}/; close F; open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/; } $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/ } END { say F q/{}]}/; close F; }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..45}.json
#  Split by 950MB
# perl -MIO::File -E 'sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && $l[0][0] <= $l[1][0])) { $txt=$l[0][1]; $sz += length($txt); if ($sz >= 1024*1024*1024) { $sz=length($txt); say F q/{}]}/; close F; open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/; } print F $txt; $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/ } END { say F q/{}]}/; close F; }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..45}.json
#  *
# perl -MIO::File -E 'sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/ || $1 < 18.3e6; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && $l[0][0] <= $l[1][0])) { $txt=$l[0][1]; $sz += length($txt); if ($sz >= 950*1024*1024) { $sz=length($txt); say F q/{}]}/; close F; open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/; } print F $txt; $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { open F, ">", "q-@{[++$fi]}.json"; say F q/{"traceEvents": [/ } END { say F q/{}]}/; close F; }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..45}.json
#  Split by time
# rm -f q-*.json; S="39.8 49 55.5 65.9" perl -MIO::File -E 'BEGIN {@S=map {$_*1e6} split /[;,:\s]/, $ENV{S}} sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && ($t=$l[0][0]) <= $l[1][0])) { if (@S && $fi <= $#S && $t >= $S[$fi]) { say F q/{}]}/; close F; open F, ">", "q-@{[$fi+1]}-$S[$fi].json"; ++$fi; say F q/{"traceEvents": [/; } print F $l[0][1]; $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { open F, ">", "q-0.json"; $fi=0; say F q/{"traceEvents": [/ } END { say F q/{}]}/; close F; }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..44}.json
#  Split by time, carry over stack
# rm -f q-*.json; S="39.8 49 55.5 65.9" perl -MIO::File -E 'BEGIN {@S=map {$_*1e6} split /[;,:\s]/, $ENV{S}} sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && ($t=$l[0][0]) <= $l[1][0])) { if (@S && $fi <= $#S && $t >= $S[$fi]) { say F q/{}]}/; close F; open F, ">", "q-@{[$fi+1]}-$S[$fi].json"; ++$fi; print F qq/{"traceEvents": [\n/, map {@$_} values %st; } if ($l[0][1] =~ /"tid": (\d++).*?"ph": "([EB])"/) { $2 eq "B" ? (push @{$st{$1}}, $l[0][1]) : pop @{$st{$1}} } print F $l[0][1]; $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { open F, ">", "q-0.json"; $fi=0; say F q/{"traceEvents": [/ } END { say F q/{}]}/; close F; }' -- calltrack-00000.json calltrack-000{30..37}.json calltrack-000{40..44}.json
#  Interleave (1 ms snapshots)
# rm -f q-i*.json; I=1000 perl -MIO::File -E 'sub nl($) { local $_; while (1) { if (!defined($_=$_[0]->getline())) { $_[0]->close(); return undef; } next if !/"ts": (\d++)/; $t=$1; return [$t, (s/, "cat": "[^"]++"//r), $_[0]]; } } @l = map {nl(IO::File->new($_, "r"))} @ARGV; while (@l) { while (@l == 1 || (@l && ($t=$l[0][0]) <= $l[1][0])) { if ($l[0][1] =~ /"tid": (\d++).*?"ph": "([EB])"/) { if ($2 eq "B") { push @{$st{$1}{L}}, $l[0][1]; } else { pop @{$st{$1}{L}}; if (@{$st{$1}{L}} < $st{$1}{Dlast}) {print($l[0][1]);--$st{$1}{Dlast};} } } if ($t >= 0+$tNext) { $tNext = $t+$ENV{I}; for (values %st) { print((@{$_->{L}})[$_->{Dlast} .. @{$_->{L}}-1]); $_->{Dlast} = @{$_->{L}}; } } $l[0] = nl($l[0][2]); shift @l if !$l[0]; } @l = sort {$a->[0] <=> $b->[0]} @l; } BEGIN { say q/{"traceEvents": [/ } END { say q/{}]}/; }' -- calltrack-0*.json | pv > q-interleave.json


# View:
# https://ui.perfetto.dev/
# chrome://tracing/
# https://github.com/jlfwong/speedscope

# Putting wait states in separate tracks in Perfetto:
# select * from slices where name like "%:WAIT%"
# ... "show debug track" ... pivot on "name"
#
# Who does eviction:
# select * from slices where name = "__wt_evict"
# Pivot on "track_id"

# select distinct name, cat from slices where name like "%:WAIT%"

_session_from_type = {
    "WT_SESSION_IMPL":    lambda arg: f"""{arg}""",
    "WT_SESSION":         lambda arg: f"""(WT_SESSION_IMPL*){arg}""",
    "WT_CURSOR":          lambda arg: f"""CUR2S({arg})""",
    "WT_CONNECTION_IMPL": lambda arg: f"""{arg}->default_session""",
    "WT_CONNECTION":      lambda arg: f"""((WT_CONNECTION_IMPL*){arg})->default_session""",
}

_int_kind_fmt = {
    "int":      ('%d',           '%x'),
    "bool":     ('%s',           '%s'),
    "float":    ('%f',           '%f'),
    "double":   ('%lf',          '%lf'),
    "int8_t":   ('%" PRIi8 "',   '0x%" PRIX8 "'),
    "int16_t":  ('%" PRIi16 "',  '0x%" PRIX16 "'),
    "int32_t":  ('%" PRIi32 "',  '0x%" PRIX32 "'),
    "int64_t":  ('%" PRIi64 "',  '0x%" PRIX64 "'),
    "uint8_t":  ('%" PRIu8 "',   '0x%" PRIX8 "'),
    "uint16_t": ('%" PRIu16 "',  '0x%" PRIX16 "'),
    "uint32_t": ('%" PRIu32 "',  '0x%" PRIX32 "'),
    "uint64_t": ('%" PRIu64 "',  '0x%" PRIX64 "'),
    "size_t":   ('%" PRIuMAX "', '0x%" PRIXMAX "'),
}

def _has_session(func: FunctionParts, func_args: list[Variable]) -> str:
    if (not func_args or
            func_args[0].typename[-1].value not in _session_from_type or
            func.name.value in ["__session_close", "__wt_session_close"] or
            "config_parser_open" in func.name.value):
        return False
    return True

def _get_session(func: FunctionParts, func_args: list[Variable]) -> str:
    return (_session_from_type[func_args[0].typename[-1].value](func_args[0].name.value)
            if _has_session(func, func_args)
            else "")

def _get_exact_type(st: Statement, maxidx: int) -> TokenList:
    ret = TokenList()
    for token in st.tokens:
        if token.idx >= maxidx:
            break
        if token.value != "static":
            ret.append(token)
    return clean_tokens_decl(ret.filterCode(), clean_static_const=False)

def _want_hex(varname: str) -> bool:
    return int(bool("flag" in varname or "hash" in varname))

def _get_int_kind_fmt(typename: str, varname: str = "__ret__") -> str:
    return (_int_kind_fmt[typename][_want_hex(varname)] if typename in _int_kind_fmt else
            '%s')

def _get_int_kind_fmt_arg(typename: str, varname: str = "__ret__") -> str:
    return (f'{varname} ? "true" : "false"' if typename == "bool" else
            varname if typename in _int_kind_fmt else
            '""')

# Count nunmber of ";" and "{" in the body
def _function_complexity(body: str) -> int:
    return body.count(";") + body.count("{")

class Patcher:
    txt = ""
    patch_list: list[tuple[tuple[int, int], int, str]]
    idx = 0  # this is used to order the patches for the same range
    count = 0

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

    def patch(self, st: Statement, func: FunctionParts) -> None:
        # if func.typename[-1].value not in ["int", "void"] or "..." in func.args.value:
        #     # We will not patch functions that return something other than int
        #     # or have variable arguments
        #     return
        if not func.typename or "..." in func.args.value:
            # We will not patch functions that have variable arguments
            return

        func_args = func.getArgs()
        complexity = _function_complexity(func.body.value)
        is_api = func.name.value.startswith("wiredtiger_") or "API_" in func.body.value
        is_wait = ("__wt_yield" in func.body.value or
                      "__wt_sleep" in func.body.value or
                      "WT_PAUSE" in func.body.value or
                      "__wt_spin_backoff" in func.body.value or
                      func.name.value in [
                            "__wt_spin_lock",
                            "__wt_readlock", "__wt_writelock",
                            "__wt_cond_auto_wait_signal", "__wt_cond_wait_signal",
                            "__wt_futex_wait",
                            "__wt_sleep"])
        is_syscall = ( #  "/src/os_" in self.file or
                      "WT_SYSCALL" in func.body.value)
        is_io = ("/include/os_fhandle_inline.h" in self.file or
                 "/os_fs.c" in self.file)
        if ("pack_" in func.name.value or
              "_destroy" in func.name.value or
              "_free" in func.name.value or
              "_atomic" in func.name.value or
              "byteswap" in func.name.value or
              func.name.value in ("__wt_abort", "__wt_yield", "__wt_thread_create", "__wt_epoch_raw") or
              func.name.value in [
                  "__block_ext_prealloc", "__block_ext_alloc", "__block_size_alloc",
                  "__block_extend", "__block_append", "__block_off_remove", "__block_ext_insert",
                  "__block_extlist_dump",
                  "__wt_compare",
                  "__config_next", "__config_merge_cmp", "__config_process_value", "__wt_config_initn",
                  "__wt_config_init", "__wt_config_next", "__config_getraw", "__config_merge_scan",
                  "__strip_comma", "__config_merge_format_next", "__wti_config_get", "__wt_config_gets",
                  "__wt_config_getones",
                  "__wt_direct_io_size_check",
                  "__wt_ref_is_root", "__ref_get_state",
                  "__wt_lex_compare", "__wt_ref_key",
                  "__cursor_pos_clear", "__wt_cursor_key_order_reset",
              ]):
            return
        elif is_api or is_wait or is_io:
            pass # instrument this function
        elif not self.mod:
            return
        # elif complexity <= 5:
        #     # Ignore too "simple" functions
        #     return

        self.count += 1

        session = _get_session(func, func_args)

        is_api_str = ":API" if is_api else ""
        is_wait_str = ":WAIT-LOCK" if is_wait else ""
        is_io_str = ":WAIT-IO" if is_io else ""
        is_syscall_str = ":WAIT-SYS" if is_syscall else ""
        static = "static " if func.is_type_static else ""
        has_ret = func.typename[-1].value != "void"
        int_ret = func.typename[-1].value == "int"
        nonint_ret = func.typename[-1].value not in ["void", "int"]
        rettype = _get_exact_type(st, func.name.idx).short_repr()
        int_like_fmt = _get_int_kind_fmt(rettype)
        int_like_fmt_arg = _get_int_kind_fmt_arg(rettype)
        printable_args = []
        for stt in StatementList.xFromText(func.args.value, base_offset=0):
            var = Variable.fromFuncArg(stt.tokens)
            if var:
                var_type = _get_exact_type(stt, var.name.idx).short_repr()
                if var_type in _int_kind_fmt:
                    printable_args.append([var.name.value,
                                           _get_int_kind_fmt(var_type, var.name.value),
                                           _get_int_kind_fmt_arg(var_type, var.name.value)])
        printable_args_str = """    memcpy(wt_calltrack_thread._args_buf, "()\\0", 4);\n""" if not printable_args else f"""
    WT_UNUSED(__wt_snprintf(wt_calltrack_thread._args_buf, sizeof(wt_calltrack_thread._args_buf),
      "({
        ", ".join((f'{name}={fmt}' for name, fmt, _ in printable_args))
    })",
      {
        ", ".join((arg for _, _, arg in printable_args))
    }));
"""

        # Insert a forward declaration of the wrapper function for the case of recursive calls
        self._replace((st.range()[0], st.range()[0]), f"""
{static}{rettype} {func.name.value}({func.args.value});
""")

        # Add "static" to the original
        if not func.is_type_static:
            self._replace((func.typename[0].range[0], func.typename[0].range[0]), f"static ")
        # Rename the original function
        self._replace(func.name.range, f"{func.name.value}__orig_")

        # Create a new function with the original name

        # /* {[arg.typename[0].value for arg in func_args]} */
        # /* {[arg.typename[-1].value for arg in func_args]} */
        # /* {[arg.name.value for arg in func_args]} */

#         self._replace((st.range()[1], st.range()[1]), f"""
# {static}{rettype}
# {func.name.value}({func.args.value}) {{
# {printable_args_str}    __WT_CALL_WRAP{"_NORET" if not has_ret else "" if int_ret else "_RET"}(
#         "{func.name.value}{is_api_str}{is_io_str}{is_wait_str}{is_syscall_str}",
#         {func.name.value}__orig_({", ".join((v.name.value for v in func_args))}),
#         {session or "NULL"}
#         {"" if not has_ret or int_ret else
#          f', {rettype}, "= {int_like_fmt}", {int_like_fmt_arg}'});
# }}
# """)

        self._replace((st.range()[1], st.range()[1]), f"""
{static}{rettype}
{func.name.value}({func.args.value}) {{
    __WT_CALL_WRAP_IMPL_BUF_GRAPH(
        "{func.name.value}{is_api_str}{is_io_str}{is_wait_str}{is_syscall_str}",
        {func.name.value}__orig_({", ".join((v.name.value for v in func_args))}),
        {session or "NULL"},
        {'; , 0, ' if not has_ret else
         f'{rettype} __ret__ = , (int64_t)__ret__, return __ret__'});
}}
""")

    def parseDetailsFromText(self, txt: str, offset: int = 0) -> None:
        with ScopePush(offset=offset):
            self.file = scope_file().name
            self.mod = fname_to_module(self.file)
            self.txt = txt
            if "/calltrack." in self.file or "/checksum/" in self.file or "/utilities/" in self.file or "/packing/" in self.file:
                return
            print(f" --- [{self.mod}] {self.file}")
            if (self.file.endswith("/include/stat.h") or
                self.file.endswith("/include/block.h")):
                return
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
    ignore_type_keywords.append("WT_STAT_MSECS_HIST_INCR_FUNC")

    files = get_files()  # list of all source files

    count = 0
    for file in files:
        parcher = Patcher()
        parcher.parseDetailsFromFile(file)
        # print(parcher.get_patched())
        # write file back
        with open(file, "w") as f:
            f.write(parcher.get_patched())
        count += parcher.count
    print(f" === Total patched functions: {count}")

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

