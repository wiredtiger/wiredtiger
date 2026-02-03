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

import re, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_disagg_checkpoint_size03.py
#   Test that the disagg total database size is updated on table drop
@disagg_test_class
class test_disagg_checkpoint_size03(wttest.WiredTigerTestCase):

    uri_base = "test_disagg_checkpoint_size03"
    conn_config = 'disaggregated=(role="leader"),disaggregated=(lose_all_my_data=true)'
    uri = "layered:" + uri_base

    def get_database_size(self):
        match = re.search(r'database_size=(\d+)', self.disagg_get_complete_checkpoint_meta())
        assert(match)
        return int(match.group(1))
    
    # Test that the database size decreases on schema drop
    def test_database_size_decreases_on_schema_drop(self):
        # Create a layered table.
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Take initial checkpoint.
        self.session.checkpoint()
        initial_size = self.get_database_size()

        # Insert some data.
        cursor = self.session.open_cursor(self.uri)
        nrows = 1000
        value_size = 100  # Each value is 100 bytes.
        for i in range(nrows):
            value = 'x' * value_size
            cursor[str(i)] = value
        cursor.close()

        # Take a checkpoint to persist the data.
        self.session.checkpoint()

        size_after_insert = self.get_database_size()

        # Size should have increased
        self.assertGreater(size_after_insert, initial_size,
            f"Database size should increase after insert: {initial_size} -> {size_after_insert}")

        # Drop the table
        self.session.drop(self.uri)

        # Take another checkpoint to ensure drop is processed.
        self.session.checkpoint()

        size_after_drop = self.get_database_size()

        self.assertLess(size_after_drop, size_after_insert,
            "Database size should decrease after table drop")
