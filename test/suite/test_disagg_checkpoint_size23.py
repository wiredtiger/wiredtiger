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

import re, wttest
from helper_disagg import disagg_test_class

# Test the database size accounting when a dropped URI is created again before the drop is
# processed. The URI alone does not identify the incarnation a removal belongs to.
@disagg_test_class
class test_disagg_checkpoint_size23(wttest.WiredTigerTestCase):
    conn_config = 'disaggregated=(role="leader",lose_all_my_data=true)'

    uri = "layered:reused_table"
    table_config = 'key_format=i,value_format=S'

    def get_database_size(self):
        match = re.search(r'database_size=(\d+)', self.disagg_get_complete_checkpoint_meta())
        assert(match)
        return int(match.group(1))

    def populate(self):
        cursor = self.session.open_cursor(self.uri)
        for i in range(1000):
            cursor[i] = 'a' * 500
        cursor.close()

    def test_recreate_between_drops_does_not_leak(self):
        self.session.create(self.uri, self.table_config)
        self.session.checkpoint()
        size_empty = self.get_database_size()

        self.populate()
        self.session.checkpoint()
        size_with_data = self.get_database_size()
        self.assertGreater(size_with_data, size_empty)

        # Take the name again before the checkpoint processes the removal, so the removal is
        # processed while the name belongs to a later incarnation. Then drop that one too: nothing
        # is left behind, so the data's size must leave the database size.
        self.session.drop(self.uri)
        self.session.create(self.uri, self.table_config)
        self.session.checkpoint()
        self.session.drop(self.uri)
        self.session.checkpoint()
        self.session.checkpoint()

        size_after = self.get_database_size()
        self.pr(f"empty={size_empty}, with_data={size_with_data}, after_all_dropped={size_after}")
        self.assertLess(size_after, size_empty + (size_with_data - size_empty) * 0.1,
            f"The dropped data must not stay in the database size: empty={size_empty}, "
            f"after_all_dropped={size_after}")

if __name__ == '__main__':
    wttest.run()
