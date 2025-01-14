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

import errno
import time
import wiredtiger
from compact_util import compact_util

# test_error_info.py
#   Test that the placeholder get_last_error() session API returns placeholder error values.
class test_error_info(compact_util):
    table_name1 = 'table:test_error'

    def check_error(self, error, sub_level_error, error_msg):
        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, error)
        self.assertEqual(sub_level_err, sub_level_error)
        self.assertEqual(err_msg, error_msg)

    def test_compaction_already_running(self):
        # Enable the background compaction server.
        self.turn_on_bg_compact()

        # Attempt to reconfigure.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.compact(None, f'background=true,free_space_target=10MB'))

        # Expect error code, sub-error code and error message to reflect compaction already running.
        self.check_error(errno.EINVAL, wiredtiger.WT_BACKGROUND_COMPACT_ALREADY_RUNNING, "Cannot reconfigure background compaction while it's already running.")

    def test_rollback_cache_overflow(self):
        # Configure the connection with an unrealistically small cache_max_wait_ms value and
        # a very low eviction trigger threshold.
        self.conn.reconfigure('cache_max_wait_ms=1,eviction_dirty_target=1,eviction_dirty_trigger=2')

        # Create a very basic table.
        self.session.create(self.table_name1, 'key_format=S,value_format=S')

        # Open a session and cursor.
        cursor = self.session.open_cursor(self.table_name1)

        # Start a transaction and insert a value large enough to trigger eviction app worker threads.
        self.session.begin_transaction()
        cursor.set_key("key_a")
        cursor.set_value("a"*1024*5000)
        cursor.update()
        self.session.commit_transaction()

        # Start a new transaction and attempt to insert a value. The very low cache_max_wait_ms
        # value should cause the eviction thread to time out.
        self.session.begin_transaction()
        cursor.set_key("key_b")
        cursor.set_value("b")

        # This reason is the default reason for WT_ROLLBACK errors so we need to catch it.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.update())

        # Expect the get_last_error reason to give us the true reason for the rollback.
        self.check_error(wiredtiger.WT_ROLLBACK, wiredtiger.WT_CACHE_OVERFLOW, "Cache capacity has overflown")

        self.ignoreStdoutPatternIfExists("transaction rolled back because of cache overflow")

    def test_rollback_write_conflict(self):
        # Create a very basic table.
        self.session.create(self.table_name1, 'key_format=S,value_format=S')

        # Insert a key and value.
        cursor = self.session.open_cursor(self.table_name1)
        self.session.begin_transaction()
        cursor.set_key("key")
        cursor.set_value("value")
        cursor.update()
        self.session.commit_transaction()
        self.session.checkpoint()
        cursor.close()

        # Update the key in the first session.
        session1 = self.session
        cursor1 = session1.open_cursor(self.table_name1)
        session1.begin_transaction()
        cursor1["key"] = "aaa"

        # Insert the same key in the second session, expect a conflict error to be produced.
        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.table_name1)
        session2.begin_transaction()
        cursor2.set_key("key")
        cursor2.set_value("bbb")

        # Catch the default reason for WT_ROLLBACK errors.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor2.update())

        # Expect the get_last_error reason to give us the true reason for the rollback.
        # The error will be set in the second session.
        self.session = session2
        self.check_error(wiredtiger.WT_ROLLBACK, wiredtiger.WT_WRITE_CONFLICT, "Write conflict between concurrent operations")

    def test_rollback_oldest_for_eviction(self):
        # Configure the connection with the min cache size.
        self.conn.reconfigure('cache_size=1MB')

        # Create a very basic table.
        self.session.create(self.table_name1, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.table_name1)

        # Start a new transaction and insert a value far too large for cache.
        self.session.begin_transaction()
        cursor.set_key("key_a")
        cursor.set_value("a"*1024*5000)
        self.assertEqual(0, cursor.update())

        # Let WiredTiger's accounting catch up.
        time.sleep(2)

        # Attempt to insert another value with the same transaction. This will result in the
        # application thread being pulled into eviction and getting rolled back.
        cursor.set_key("key_b")
        cursor.set_value("b"*1024)

        # Catch the default reason for WT_ROLLBACK errors.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.update())

        # Expect the get_last_error reason to give us the true reason for the rollback.
        self.check_error(wiredtiger.WT_ROLLBACK, wiredtiger.WT_OLDEST_FOR_EVICTION, "Transaction has the oldest pinned transaction ID")

    def test_uncommitted_data(self):
        # Create a simple table.
        self.session.create(self.table_name1, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.table_name1)

        # Start a transaction and insert a key and value.
        self.session.begin_transaction()
        cursor.set_key('key')
        cursor.set_value('value')
        cursor.insert()
        cursor.close()

        # Attempt to drop the table without committing the transaction.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.drop(self.table_name1, None))

        # Expect error code, sub-error code and error message to reflect uncommitted data.
        self.check_error(errno.EBUSY, wiredtiger.WT_UNCOMMITTED_DATA, "the table has uncommitted data and cannot be dropped yet")

    def test_dirty_data(self):
        # Create a simple table.
        self.session.create(self.table_name1, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.table_name1)

        # Start a transaction, insert a key and value, and commit the transaction.
        self.session.begin_transaction()
        cursor.set_key('key')
        cursor.set_value('value')
        self.assertEqual(cursor.update(), 0)
        self.assertEqual(self.session.commit_transaction(), 0)
        cursor.close()

        # Give time for the oldest id to update.
        time.sleep(1)

        # Attempt to drop the table without performing a checkpoint.
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.drop(self.table_name1, None))

        # Expect error code, sub-error code and error message to reflect dirty data.
        self.check_error(errno.EBUSY, wiredtiger.WT_DIRTY_DATA, "the table has dirty data and can not be dropped yet")

    def test_error_info(self):
        self.check_error(0, wiredtiger.WT_NONE, "")

    def test_invalid_config(self):
        expectMessage = 'unknown configuration key'
        with self.expectedStderrPattern(expectMessage):
            try:
                self.session.create('table:' + self.table_name1,
                                    'expect_this_error,okay?')
            except wiredtiger.WiredTigerError as e:
                self.assertTrue(str(e).find('nvalid argument') >= 0)

        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, errno.EINVAL)
        self.assertEqual(sub_level_err, wiredtiger.WT_NONE)
        self.assertEqual(err_msg, "unknown configuration key 'expect_this_error'")
