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
    # Disable the obsolete time window path so only fully deleted pages drive cleanup reads.
    conn_config = ("statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0),"
        "heuristic_controls=(checkpoint_cleanup_obsolete_tw_pages_dirty_max=0)")
    nrows = 20000

    # Each scenario removes the first half of every group of keys: small groups leave every leaf
    # with live keys, large groups fully delete runs of leaves scattered through a multi-level tree.
    scenarios = make_scenarios([
        ("partial_remove", dict(group=20, full_pages=False)),
        ("mixed_pages_remove", dict(group=800, full_pages=True)),
        ("full_remove", dict(group=nrows * 2, full_pages=True)),
    ])

    def remove_ranges(self, cursor):
        for start in range(0, self.nrows, self.group):
            self.session.begin_transaction()
            for key in range(start, min(start + self.group // 2, self.nrows)):
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction(
                "commit_timestamp=" + self.timestamp_str(self.nrows + 1))

    def test_cc12(self):
        uri = "table:cc12"
        nrows = self.nrows
        create_params = (
            "key_format=i,value_format=S,"
            "allocation_size=512,leaf_page_max=4KB,internal_page_max=512")

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, "k" * 40)
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows))
        self.session.checkpoint()
        self.reopen_conn()

        cursor = self.session.open_cursor(uri)
        self.remove_ranges(cursor)
        cursor.close()

        cursor = self.session.open_cursor(uri)
        remaining = sum(1 for _ in cursor)
        cursor.close()
        removed = sum(min(self.group // 2, nrows - start) for start in range(0, nrows, self.group))
        self.assertEqual(remaining, nrows - removed)

        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows + 2))
        self.session.checkpoint()
        self.reopen_conn()
        block_preload_before = self.get_stat(stat.conn.block_preload)
        keep_open = self.session.open_cursor(uri)
        block_preload_after = self.get_stat(stat.conn.block_preload)

        self.conn.set_timestamp("oldest_timestamp=" + self.timestamp_str(nrows + 2))
        pages_read_before = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri)
        pages_removed_before = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_removed, uri)
        self.wait_for_cc_to_run()
        pages_read = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri) - pages_read_before
        pages_removed = \
            self.get_stat(stat.dsrc.checkpoint_cleanup_pages_removed, uri) - pages_removed_before
        keep_open.close()

        if self.full_pages:
            self.assertGreater(pages_read, 0)
            self.assertGreater(pages_removed, 0)
        else:
            self.assertEqual(pages_read, 0)
            self.assertEqual(pages_removed, 0)
        if self.group > nrows:
            self.assertEqual(block_preload_after - block_preload_before, 0)


if __name__ == "__main__":
    test_cc12.run()
