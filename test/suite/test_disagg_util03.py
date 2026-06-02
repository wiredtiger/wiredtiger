#!/usr/bin/env python
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

import wttest
from helper_disagg import DisaggConfigMixin
from suite_subprocess import suite_subprocess

# Test the `wt turtle` command against a palite backed disaggregated storage
# database. A leader connection writes a checkpoint, then `wt turtle` is run as
# a subprocess in follower mode against the same cell to inspect the turtle
# blob returned by pl_get_complete_checkpoint.
@wttest.skip_for_hook("tiered", "wt turtle does not run under tiered hook")
class test_disagg_util03(wttest.WiredTigerTestCase, suite_subprocess, DisaggConfigMixin):
    uri = "layered:wt_turtle_test"
    nrows = 100

    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _follower_config(self):
        return self.extensionsConfig() + ',disaggregated=(role="follower")'

    # Returns (stdout, stderr); failure=True asserts a non-zero exit.
    def _run_wt_turtle(self, *args, failure=False):
        cmd = ['-C', self._follower_config(), 'turtle'] + list(args)
        self.runWt(cmd, outfilename='wt.out', errfilename='wt.err',
                   failure=failure)
        with open('wt.out') as f:
            stdout = f.read()
        with open('wt.err') as f:
            stderr = f.read()
        return stdout, stderr

    def _populate_and_checkpoint(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def test_latest_turtle(self):
        self._populate_and_checkpoint()
        self.close_conn()

        stdout, _ = self._run_wt_turtle()
        self.assertIn('=== turtle ===', stdout)
        self.assertRegex(stdout, r'(?m)^lsn=\d+$')
        self.assertRegex(stdout, r'(?m)^metadata_lsn=\d+$')
        self.assertRegex(stdout, r'(?m)^metadata_checksum=0x[0-9a-f]+$')
        self.assertRegex(stdout, r'(?m)^database_size=\d+$')
        self.assertRegex(stdout, r'(?m)^version=\d+$')
        self.assertRegex(stdout, r'(?m)^compatible_version=\d+$')

        # Chase should fire automatically in default mode.
        self.assertRegex(stdout, r'(?m)^=== metadata page \(table_id=2, page_id=1, lsn=\d+\) ===$')
        self.assertIn('checksum=OK', stdout)
        # The shared metadata page is a config string that always carries `checkpoint=`.
        self.assertIn('checkpoint=', stdout)

if __name__ == '__main__':
    wttest.run()
