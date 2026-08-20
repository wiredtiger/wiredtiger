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

import os
import wttest
from helper_disagg import disagg_test_class
from run import wt_builddir
from suite_subprocess import suite_subprocess

# Verify the wt CLI rejects global flags that are not supported in
# disaggregated storage mode.
@disagg_test_class
class test_disagg_util06(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config = 'disaggregated=(role="leader")'

    REJECT_MSG = 'is not supported in disaggregated storage mode'

    def _rejected_cases(self):
        return [
            ('-B', []),
            ('-E', ['dummy_key']),
            ('-L', []),
            ('-l', [os.path.join(self.home, 'no-such-live-restore')]),
            ('-R', []),
            ('-S', []),
        ]

    def _disagg_extension_path(self):
        ext_dir = os.path.join(wt_builddir, 'ext', 'page_log', self.ds_name)
        candidates = [os.path.join(ext_dir, e) for e in os.listdir(ext_dir)
                      if e.endswith('.so') or e.endswith('.dylib')]
        self.assertEqual(len(candidates), 1,
            f"expected exactly one page-log shared object under {ext_dir}, got {candidates}")
        return candidates[0]

    def _follower_setup(self, name):
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.close_conn()

        follower_home = os.path.join(self.home, name)
        os.mkdir(follower_home)
        os.symlink('../kv_home', os.path.join(follower_home, 'kv_home'), target_is_directory=True)

        ext_path = self._disagg_extension_path()
        page_log = self.page_log()
        config = (f'create,'
                  f'extensions=[{ext_path}=(config="(verbose=0)")],'
                  f'disaggregated=(role="follower",page_log={page_log})')
        return follower_home, config

    def _leader_prepare_checkpoint(self):
        # A completed checkpoint is required so the follower could otherwise
        # attach and fail cleanly.
        self.session.create('layered:test_disagg_util06', 'key_format=S,value_format=S')
        self.session.checkpoint()

    def test_reject_flags(self):
        self._leader_prepare_checkpoint()
        follower_home, config = self._follower_setup('wt-follower')

        for flag, flag_args in self._rejected_cases():
            outfile = f'wt{flag}.out'
            errfile = f'wt{flag}.err'
            self.runWt(
                [flag] + flag_args + ['-h', follower_home, '-C', config, 'list'],
                outfilename=outfile, errfilename=errfile,
                closeconn=False, failure=True)
            with open(errfile) as f:
                err = f.read()
            self.assertIn(f'{flag} {self.REJECT_MSG}', err,
                f"expected reject message for {flag}, got stderr:\n{err}")

    def test_accept_supported_flag(self):
        self._leader_prepare_checkpoint()
        follower_home, config = self._follower_setup('wt-follower-ok')

        self.runWt(['-v', '-h', follower_home, '-C', config, 'list'],
            outfilename='wtok.out', errfilename='wtok.err', closeconn=False)
        with open('wtok.err') as f:
            err = f.read()
        self.assertNotIn(self.REJECT_MSG, err)
