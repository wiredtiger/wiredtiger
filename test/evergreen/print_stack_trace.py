#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import argparse
import itertools
import os
import re
import subprocess
import sys
from shutil import which


def border_msg(msg: str):
    count = len(msg) + 2
    dash = "-" * count
    return "+{dash}+\n| {msg} |\n+{dash}+".format(dash=dash, msg=msg)


class LLDBDumper:
    """LLDBDumper class - prints stack traces on macOS"""
    def __init__(self):
        self.dbg = self._find_debugger("lldb")

    @staticmethod
    def _find_debugger(debugger: str):
        """Find the installed debugger."""
        return which(debugger)

    def dump(self, exe_path: str, core_path: str, dump_all: bool, output_file: str):
        """Dump stack trace."""
        if self.dbg is None:
            sys.exit("Debugger lldb not found,"
                     "skipping dumping of {}".format(core_path))

        if dump_all:
            cmds.append("thread apply all backtrace -c 30")
        else:
            cmds.append("backtrace -c 30")
        cmds.append("quit")

        output = None
        if (output_file):
            output = open(output_file, "w")
        subprocess.run([self.dbg, "--batch"] + [exe_path, "-c", core_path] +
                       list(itertools.chain.from_iterable([['-o', b] for b in cmds])),
                       check=True, stdout=output)


class GDBDumper:
    """GDBDumper class - prints stack traces on Linux"""
    def __init__(self):
        self.dbg = self._find_debugger("gdb")

    @staticmethod
    def _find_debugger(debugger: str):
        """Find the installed debugger."""
        return which(debugger)

    def dump(self, exe_path: str, core_path: str, lib_path: str, dump_all: bool, output_file: str):
        """Dump stack trace."""
        if self.dbg is None:
            sys.exit("Debugger gdb not found,"
                     "skipping dumping of {}".format(core_path))

        cmds = []
        if lib_path:
            cmds.append("set solib-search-path " + lib_path)

        if dump_all:
            cmds.append("thread apply all backtrace 30")
        else:
            cmds.append("backtrace 30")
        cmds.append("quit")

        output = None
        if (output_file):
            output = open(output_file, "w")
        subprocess.run([self.dbg, "--batch", "--quiet"] +
                       list(itertools.chain.from_iterable([['-ex', b] for b in cmds])) +
                       [exe_path, core_path],
                       check=True, stdout=output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--core_path',
                        help='directory path to the core dumps')
    parser.add_argument('-e', '--executable_path',
                        help='path to the executable')
    parser.add_argument('-l', '--lib_path', help='library path')
    args = parser.parse_args()

    # Store the path of the core files as a list.
    core_files = []
    regex = re.compile(r'dump.*core', re.IGNORECASE)
    core_path = args.core_path
    if core_path is None:
        core_path = '.'

    for root, _, files in os.walk(core_path):
        for file in files:
            if regex.match(file):
                core_files.append(os.path.join(root, file))

    for core_file_path in core_files:
        print(border_msg(core_file_path), flush=True)

        # If not specified, derive the binary name from the core dump folder.
        # The test/format and cpp suites are different and need specific handling. All the other
        # tests have an executable that follows the pattern "test_<test_folder>". For example,
        # the test folder wt2853_perf contains an executable named test_wt2853_perf.
        if args.executable_path is None:
            # Retrieve the first part of the path without the core name.
            dirname = core_file_path.rsplit('/', 1)[0]
            if dirname.startswith('WT_TEST/test_'):
                executable_path = sys.executable
            elif 'cppsuite' in core_file_path:
                executable_path = dirname + "/run"
            elif 'format' in core_file_path:
                executable_path = dirname + "/t"
            else:
                test_name = dirname.rsplit('/', 1)[1]
                executable_path = dirname + "/test_" + test_name
        else:
            executable_path = args.executable_path

        dump_all = True
        # Extract the filename from the core file path, to create a stacktrace output file.
        file_name, _ = os.path.splitext(os.path.basename(core_file_path))
        file_name = file_name + ".stacktrace.txt"

        if sys.platform.startswith('linux'):
            dbg = GDBDumper()
            dbg.dump(executable_path, core_file_path, args.lib_path, dump_all, None)
            dbg.dump(executable_path, core_file_path, args.lib_path, dump_all, file_name)
        elif sys.platform.startswith('darwin'):
            # FIXME - macOS to be supported in WT-8976
            # dbg = LLDBDumper()
            # dbg.dump(executable_path, core_file_path, dump_all, None)
            # dbg.dump(executable_path, core_file_path, args.lib_path, dump_all, file_name)
            pass
        elif sys.platform.startswith('win32') or sys.platform.startswith('cygwin'):
            # FIXME - Windows to be supported in WT-8937
            pass

if __name__ == "__main__":
    main()
