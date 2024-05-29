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

from test_cc01 import test_cc_base
from wiredtiger import stat

# test_cc07.py
# Verify checkpoint cleanup removes the obsolete time window from the pages.
class test_cc07(test_cc_base):
    conn_config = 'statistics=(all),statistics_log=(json,wait=0,on_close=true)'

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def populate(self, uri, start_key, num_keys, value_size=1024):
        c = self.session.open_cursor(uri, None)
        for k in range(start_key, num_keys):
            self.session.begin_transaction()
            c[k] = 'k' * value_size
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(k + 1))
        c.close()

    def test_cc(self):
        create_params = 'key_format=i,value_format=S'
        nrows = 10000
        uri = 'table:cc07'
        value_size = 1024

        self.session.create(uri, create_params)

        for i in range(10):
            # Append some data.
            self.populate(uri, nrows * (i), nrows * (i + 1))

            # Checkpoint with cleanup.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(nrows * (i + 1)))
            self.session.checkpoint("debug=(checkpoint_cleanup=true)")
            self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(nrows * (i + 1)))

        self.session.checkpoint("debug=(checkpoint_cleanup=true)")
        self.wait_for_cc_to_run()

        # Check statistics.
        self.assertGreater(self.get_stat(stat.conn.cc_pages_obsolete_timewindow), 0)
