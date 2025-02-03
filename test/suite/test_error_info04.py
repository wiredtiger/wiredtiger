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

import wiredtiger, time
from error_info_util import error_info_util

# test_error_info04.py
#   Test that when committing or rolling back a transaction, after successfully committing or
#   rolling back, if an error occurs in __wti_evict_app_assist_worker it is not saved in err_info.
class test_error_info04(error_info_util):
    uri = "table:test_error_info.wt"

    def test_commit_transaction_skip_save(self):
        # Configure connection with very low cache wait time and dirty trigger.
        self.conn.reconfigure('cache_max_wait_ms=2,eviction_dirty_target=1,eviction_dirty_trigger=2')

        # Create a basic table.
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Open a session and cursor.
        cursor = self.session.open_cursor(self.uri)

        # Start a transaction and insert a value large enough to trigger eviction app worker threads.
        with self.expectedStdoutPattern("transaction rolled back because of cache overflow"):
            for i in range(100):
                self.session.begin_transaction()
                cursor.set_key(str(i))
                cursor.set_value(str(i)*1024*500)
                cursor.insert()
                self.assertEqual(self.session.commit_transaction(), 0)
                self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")

        self.session.checkpoint()

    def test_rollback_transaction_skip_save(self):
        # Configure connection with very low cache max wait time and dirty trigger.
        self.conn.reconfigure('cache_max_wait_ms=2,eviction_dirty_target=1,eviction_dirty_trigger=2')

        # Create a basic table.
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Open a session and cursor.
        cursor = self.session.open_cursor(self.uri)

        # Insert a key and value within a transaction.

        # Start a transaction and insert a value large enough to trigger eviction app worker threads.
        with self.expectedStdoutPattern("transaction rolled back because of cache overflow"):
            for i in range(100):
                self.session.begin_transaction()
                cursor.set_key(str(i))
                cursor.set_value(str(i)*1024*500)
                cursor.insert()
                self.assertEqual(self.session.rollback_transaction(), 0)
                self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")
