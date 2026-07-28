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

import os, re, subprocess
import wttest
from helper_disagg import disagg_test_class
from run import wt_builddir
from suite_subprocess import suite_subprocess

# Verify that the wt CLI enforces the disaggregated-storage subcommand
# allowlist end-to-end.

@disagg_test_class
class test_disagg_util05(wttest.WiredTigerTestCase, suite_subprocess):
    # Keep in sync with util_func_allowed_disagg() in src/utilities/util_main.c.
    ALLOWED_SUBCOMMANDS = frozenset(
        ('dump', 'list', 'page', 'read', 'stat', 'turtle', 'verify'))

    # Subcommands that short-circuit in main() before wiredtiger_open() runs,
    # so the disagg reject path never sees them. Skipped from the survey.
    NO_STORAGE_ACCESS = frozenset(('copyright',))

    REJECT_MSG = 'is not supported in disaggregated storage mode'

    conn_config = 'disaggregated=(role="leader")'

    def _wt_binary(self):
        libs_wt = os.path.join(wt_builddir, '.libs', 'wt')
        return libs_wt if os.path.isfile(libs_wt) else os.path.join(wt_builddir, 'wt')

    # Parse the "commands:" section of `wt -?` output for the list of
    # subcommands the utility knows about. util_usage() indents each
    # subcommand name with four spaces and its description with eight.
    def _all_subcommands(self):
        result = subprocess.run(
            [self._wt_binary(), '-?'],
            capture_output=True, text=True)
        subcmds = []
        in_commands = False
        for line in result.stderr.splitlines():
            if line.startswith('commands:'):
                in_commands = True
                continue
            if not in_commands:
                continue
            m = re.match(r'^    ([a-z_]+)\s*$', line)
            if m:
                subcmds.append(m.group(1))
        return subcmds

    def _disagg_extension_path(self):
        ext_dir = os.path.join(wt_builddir, 'ext', 'page_log', self.ds_name)
        candidates = [os.path.join(ext_dir, e) for e in os.listdir(ext_dir)
                      if e.endswith('.so') or e.endswith('.dylib')]
        self.assertEqual(len(candidates), 1,
            f"expected exactly one page-log shared object under {ext_dir}, got {candidates}")
        return candidates[0]

    # Spawn `wt <args>` against a sibling home that shares kv_home with the
    # leader. runWt cannot be used here because its exit-code assertions do
    # not fit a survey that mixes rejected (exit 0) and allowed-but-argless
    # (exit != 0) subcommands.
    def _run_wt_follower(self, name, wt_args):
        follower_home = os.path.join(self.home, name)
        os.mkdir(follower_home)
        os.symlink('../kv_home', os.path.join(follower_home, 'kv_home'),
            target_is_directory=True)

        ext_path = self._disagg_extension_path()
        page_log = self.page_log()
        config = (f'create,'
                  f'extensions=[{ext_path}=(config="(verbose=0)")],'
                  f'disaggregated=(role="follower",page_log={page_log})')
        cmd = [self._wt_binary(), '-h', follower_home, '-C', config] + list(wt_args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout, result.stderr

    def test_allowlist_matches_binary(self):
        # A checkpoint is required so the follower can attach.
        self.session.create('layered:test_disagg_util05',
            'key_format=S,value_format=S')
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.close_conn()

        subcmds = self._all_subcommands()
        self.assertGreater(len(subcmds), 0,
            "failed to parse any subcommands from `wt -?` output")

        for cmd in subcmds:
            if cmd in self.NO_STORAGE_ACCESS:
                continue
            stdout, stderr = self._run_wt_follower(f'wt-{cmd}', [cmd])
            rejected = self.REJECT_MSG in stderr
            if cmd in self.ALLOWED_SUBCOMMANDS:
                self.assertFalse(rejected,
                    f"subcommand '{cmd}' is on the allowlist but was rejected;\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}")
            else:
                self.assertTrue(rejected,
                    f"subcommand '{cmd}' is not on the allowlist but was not rejected;\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}")
