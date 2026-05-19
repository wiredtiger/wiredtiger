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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_fast_truncate import LayeredFastTruncateConfigMixin
from wtscenario import make_scenarios
from wiredtiger import stat

# test_layered_fast_truncate17.py
#   Verify that step-up replay uses fast page truncation (WT_REF_DELETED) when
#   replaying follower truncates.
@disagg_test_class
class test_layered_fast_truncate17(LayeredFastTruncateConfigMixin, wttest.WiredTigerTestCase):

    conn_config = 'disaggregated=(role="leader")'
    uri = 'layered:test_layered_ft_replay'
    table_config = 'key_format=i,value_format=S,leaf_page_max=4096'
    nitems = 5000

    disagg_storages = gen_disagg_storages('test_layered_ft_replay', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def setup_follower(self):
        self.setup_dual_conn_follower(
            table_config=self.table_config, statistics=True, set_oldest=True)

    def assert_fast_truncate_fired(self, msg):
        before = self.get_stat(self.conn_follow, stat.conn.rec_page_delete_fast)
        self.step_up()
        after = self.get_stat(self.conn_follow, stat.conn.rec_page_delete_fast)
        self.assertGreater(after, before, msg)

    def test_fast_truncate_fires_during_replay(self):
        self.setup_follower()
        # Leave boundary pages untouched so interior pages are eligible for fast-delete.
        trunc_start, trunc_stop = 200, self.nitems - 200 - 1
        self.truncate(trunc_start, trunc_stop, commit_timestamp=20, session=self.session_follow)
        self.assert_fast_truncate_fired("Fast truncate did not happen.")
        self.assert_ranges_deleted(self.session_follow, [(trunc_start, trunc_stop)], ts=30)

    def test_fast_truncate_multiple_ranges(self):
        self.setup_follower()
        self.truncate(200, 1199, commit_timestamp=20, session=self.session_follow)
        self.truncate(1700, 2699, commit_timestamp=20, session=self.session_follow)
        self.truncate(3200, 4199, commit_timestamp=20, session=self.session_follow)
        self.assert_fast_truncate_fired(
            "fast truncate did not increase for multi-range truncate.")
        self.assert_ranges_deleted(self.session_follow,
            [(200, 1199), (1700, 2699), (3200, 4199)], ts=30)
