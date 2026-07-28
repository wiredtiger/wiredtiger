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

import os, re
import wttest
from helper_disagg import disagg_test_class
from run import wt_builddir
from suite_subprocess import suite_subprocess

# Verify that the wt CLI rejects the disaggregated-storage-unsupported
# subcommands end-to-end. Framed as a reject list rather than an allowlist:
# support for existing subcommands is expected to shift more often than the
# set of subcommands itself, and a subcommand becoming supported means one
# entry drops from this list rather than a rebalance across both sides.

@disagg_test_class
class test_disagg_util05(wttest.WiredTigerTestCase, suite_subprocess):
    # Keep in sync with util_func_allowed_disagg() in src/utilities/util_main.c
    # (this set is that function's complement over the wt subcommand table,
    # minus the no-storage-access set below).
    REJECTED_SUBCOMMANDS = frozenset(('alter', 'backup', 'compact', 'create',
        'downgrade', 'drop', 'load', 'loadtext', 'printlog', 'salvage',
        'truncate', 'write'))
    NO_STORAGE_ACCESS = frozenset(('copyright',))

    REJECT_MSG = 'is not supported in disaggregated storage mode'

    conn_config = 'disaggregated=(role="leader")'

    def _disagg_extension_path(self):
        ext_dir = os.path.join(wt_builddir, 'ext', 'page_log', self.ds_name)
        candidates = [os.path.join(ext_dir, e) for e in os.listdir(ext_dir) if e.endswith('.so')]
        self.assertEqual(len(candidates), 1)
        return candidates[0]

    # Parse the "commands:" section of `wt -?` output for every subcommand the
    # utility knows about. util_usage() indents each subcommand name with four
    # spaces and its description with eight.
    def _all_subcommands(self, follower_home, follower_config):
        errfile = 'wt-help.err'
        self.runWt(['-h', follower_home, '-C', follower_config, '-?'],
                   outfilename='wt-help.out', errfilename=errfile, closeconn=False)
        subcmds = []
        in_commands = False
        with open(errfile) as f:
            for line in f:
                if line.startswith('commands:'):
                    in_commands = True
                    continue
                if not in_commands:
                    continue
                m = re.match(r'^    ([a-z_]+)\s*$', line)
                if m:
                    subcmds.append(m.group(1))
        return subcmds

    # Run `wt <args>` in follower mode against the shared follower home.
    # Passing outfilename/errfilename suppresses the framework's implicit
    # empty-file assertions; `[cmd, '-?']` returns 0 for both allowed
    # (subcommand-level -? handler) and rejected (util_main goto done) paths,
    # so the default failure=False exit-code check is uniform.
    def _run_wt_follower(self, follower_home, follower_config, out_tag, wt_args):
        outfile, errfile = f'{out_tag}.out', f'{out_tag}.err'
        self.runWt(['-h', follower_home, '-C', follower_config] + list(wt_args),
                   outfilename=outfile, errfilename=errfile, closeconn=False)
        with open(outfile) as f:
            out = f.read()
        with open(errfile) as f:
            err = f.read()
        return out, err

    def test_reject_list(self):
        # A checkpoint is required so the follower can attach.
        self.session.create('layered:test_disagg_util05',
            'key_format=S,value_format=S')
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.close_conn()

        follower_home = os.path.join(self.home, 'wt-follower')
        os.mkdir(follower_home)
        os.symlink('../kv_home', os.path.join(follower_home, 'kv_home'),
            target_is_directory=True)
        follower_config = (
            f'create,extensions=[{self._disagg_extension_path()}=(config="(verbose=0)")],'
            f'disaggregated=(role="follower",page_log={self.page_log()})')

        subcmds = self._all_subcommands(follower_home, follower_config)
        self.assertGreater(len(subcmds), 0)

        for cmd in subcmds:
            if cmd in self.NO_STORAGE_ACCESS:
                continue
            _, stderr = self._run_wt_follower(follower_home, follower_config, f'wt-{cmd}',
                [cmd, '-?'])
            if cmd in self.REJECTED_SUBCOMMANDS:
                self.assertIn(self.REJECT_MSG, stderr)
            else:
                self.assertNotIn(self.REJECT_MSG, stderr)
