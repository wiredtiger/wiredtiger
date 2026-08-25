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
from test_cc01 import test_cc_base
from wiredtiger import stat
from wtscenario import make_scenarios


@wttest.skip_for_hook("tiered", "Checkpoint cleanup does not support tiered tables")
class test_cc12(test_cc_base):
    conn_config = "statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0)"
    scenarios = make_scenarios([
        ("partial_remove", dict(remove_all=False)),
        ("full_remove", dict(remove_all=True)),
    ])

    def remove_ranges(self, cursor, nrows):
        if self.remove_all:
            self.session.begin_transaction()
            for key in range(nrows):
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction(
                "commit_timestamp=" + self.timestamp_str(nrows + 1))
            return

        for start in range(0, nrows, 20):
            self.session.begin_transaction()
            for key in range(start, min(start + 10, nrows)):
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction(
                "commit_timestamp=" + self.timestamp_str(nrows + 1))

    def test_cc12(self):
        uri = "table:cc12"
        nrows = 1000
        create_params = (
            "key_format=i,value_format=S,"
            "allocation_size=512,leaf_page_max=4KB,internal_page_max=4KB")

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, "k" * 40)
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows))
        self.session.checkpoint()
        self.reopen_conn()

        cursor = self.session.open_cursor(uri)
        self.remove_ranges(cursor, nrows)
        cursor.close()

        cursor = self.session.open_cursor(uri)
        remaining = sum(1 for _ in cursor)
        cursor.close()
        self.assertEqual(remaining, 0 if self.remove_all else nrows // 2)

        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows + 2))
        self.session.checkpoint()
        self.reopen_conn()
        keep_open = self.session.open_cursor(uri)

        pages_read_before = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri)
        self.wait_for_cc_to_run()
        pages_read_after = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri)

        self.assertEqual(pages_read_after - pages_read_before, 0)
        keep_open.close()


if __name__ == "__main__":
    test_cc12.run()
