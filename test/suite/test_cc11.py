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


# Verify checkpoint cleanup processes timestamped live pages after their timestamps become old.
class test_cc11(test_cc_base):
    conn_config = 'statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0)'

    def test_timestamped_insert_cleanup(self):
        uri = 'table:cc11'
        nrows = 10000
        create_params = 'key_format=i,value_format=S,allocation_size=512,leaf_page_max=512,internal_page_max=512'

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, 'value', 10)
        self.session.checkpoint()

        # Cleanup only considers handles that are open after the restart.
        self.reopen_conn()
        self.conn.set_timestamp('oldest_timestamp=11')
        keep_open = self.session.open_cursor(uri)

        pages_before = self.get_stat(stat.conn.checkpoint_cleanup_pages_read_obsolete_tw)
        visited_before = self.get_stat(stat.conn.checkpoint_cleanup_pages_visited)
        self.wait_for_cc_to_run()
        pages_after = self.get_stat(stat.conn.checkpoint_cleanup_pages_read_obsolete_tw)
        visited_after = self.get_stat(stat.conn.checkpoint_cleanup_pages_visited)
        handles_processed = self.get_stat(stat.conn.checkpoint_cleanup_handle_processed)

        self.assertGreater(handles_processed, 0)
        self.assertGreater(visited_after, visited_before)
        self.assertGreater(pages_after, pages_before)
        keep_open.close()


if __name__ == '__main__':
    test_cc11.run()
