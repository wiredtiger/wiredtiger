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

import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, DisaggCorruptionMixin
from suite_subprocess import suite_subprocess

# Reading individual pages in follower mode without a checkpoint pickup
# (WT-17349). The tool must start when the checkpoint is corrupt, and
# `wt page -t` must read intact data pages directly off the page log.
@wttest.skip_for_hook("tiered", "wt page does not run under tiered hook")
class test_disagg_util03(wttest.WiredTigerTestCase, suite_subprocess,
                         DisaggConfigMixin, DisaggCorruptionMixin):
    uri = "layered:util03"
    stable_uri = "file:util03.wt_stable"
    nrows = 1000
    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _follower_config(self):
        return self.extensionsConfig() + ',disaggregated=(role="follower")'

    def _run_wt(self, *args, failure=False):
        cmd = ['-C', self._follower_config()] + list(args)
        # reopensession=False: the Python connection stays closed after wt exits;
        # the test does not need it again, and the corrupt checkpoint would make
        # open_conn() fail anyway.
        self.runWt(cmd, outfilename='wt.out', errfilename='wt.err',
                   failure=failure, reopensession=False)
        with open('wt.out') as f:
            out = f.read()
        with open('wt.err') as f:
            err = f.read()
        return out, err

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def test_tool_starts_with_corrupt_checkpoint(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        self._populate()
        self.corrupt_checkpoint_metadata_page()
        # `wt list` needs no page data; with empty metadata it lists nothing
        # and must exit zero, with a warning rather than an abort.
        _, err = self._run_wt('list')
        self.assertIn('proceeding with empty metadata', err)

if __name__ == '__main__':
    wttest.run()
