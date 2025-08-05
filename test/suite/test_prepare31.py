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

# Tests checkpoint behavior with aborted prepared transactions based on stable timestamp:
# - Skip writing aborted prepared updates when rollback timestamp is stable
# - Skip writing when prepare timestamp is not stable
# - Write prepared updates when prepare timestamp is stable but rollback timestamp is not

class test_prepare31(wttest.WiredTigerTestCase):
    conn_config = 'checkpoint=(precise=true),preserve_prepared=true,statistics=(all)'

    def test_skip_aborted_prepare_update_if_stable_rollback_timestamp(self):
        # Set initial timestamps - start with lower values
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        uri = 'table:test_prepare31'
        create_params = 'key_format=i,value_format=S'
        self.session.create(uri, create_params)

        # Insert some initial data that will be committed
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 100):
            cursor.set_key(i)
            cursor.set_value("initial_value_" + str(i))
            cursor.insert()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Advance stable timestamp after the commit
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))

        # Verify initial data is there
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))
        cursor.set_key(50)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "initial_value_50")
        self.session.commit_transaction()
        cursor.close()

        # Start a prepared transaction that will be aborted
        session_prepare = self.conn.open_session()
        cursor_prepare = session_prepare.open_cursor(uri)
        session_prepare.begin_transaction()

        # Make updates in the prepared transaction
        for i in range(1, 100):
            cursor_prepare[i] = "prepared_value_" + str(i)

        session_prepare.prepare_transaction('prepare_timestamp=' + self.timestamp_str(70)+',prepared_id=' + self.prepared_id_str(1))

        session_prepare.rollback_transaction('rollback_timestamp=' + self.timestamp_str(80))

        # This makes the rollback timestamp "stable"
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(90))

        # Force checkpoint to write data to disk - this should skip the aborted prepared updates
        # since their rollback timestamp (80) is less than stable timestamp (90)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        rec_time_window_prepared = stat_cursor[wiredtiger.stat.conn.rec_time_window_prepared][2]
        self.assertEqual(rec_time_window_prepared, 0)

        stat_cursor.close()
        cursor_prepare.close()
        session_prepare.close()

    def test_skip_aborted_prepare_update_if_prepare_timestamp_not_stable(self):
        # Set initial timestamps - start with lower values
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        uri = 'table:test_prepare31'
        create_params = 'key_format=i,value_format=S'
        self.session.create(uri, create_params)

        # Insert some initial data that will be committed
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 100):
            cursor.set_key(i)
            cursor.set_value("initial_value_" + str(i))
            cursor.insert()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Advance stable timestamp after the commit
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))

        # Verify initial data is there
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))
        cursor.set_key(50)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "initial_value_50")
        self.session.commit_transaction()
        cursor.close()

        # Start a prepared transaction that will be aborted
        session_prepare = self.conn.open_session()
        cursor_prepare = session_prepare.open_cursor(uri)
        session_prepare.begin_transaction()

        # Make updates in the prepared transaction
        for i in range(1, 100):
            cursor_prepare[i] = "prepared_value_" + str(i)

        # Prepare the transaction with timestamp 70
        session_prepare.prepare_transaction('prepare_timestamp=' + self.timestamp_str(70)+',prepared_id=' + self.prepared_id_str(1))

        # Abort the prepared transaction with rollback timestamp 80
        session_prepare.rollback_transaction('rollback_timestamp=' + self.timestamp_str(80))

        # Force checkpoint to write data to disk - this should skip the aborted prepared updates
        # since their prepare timestamp is after stable timestamp
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        rec_time_window_prepared = stat_cursor[wiredtiger.stat.conn.rec_time_window_prepared][2]
        self.assertEqual(rec_time_window_prepared, 0)

        stat_cursor.close()
        cursor_prepare.close()
        session_prepare.close()

    def test_write_prepare_update_if_rollback_timestamp_not_stable(self):
        # Set initial timestamps - start with lower values
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        uri = 'table:test_prepare31'
        create_params = 'key_format=i,value_format=S'
        self.session.create(uri, create_params)

        # Insert some initial data that will be committed
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 100):
            cursor.set_key(i)
            cursor.set_value("initial_value_" + str(i))
            cursor.insert()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Advance stable timestamp after the commit
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))

        # Verify initial data is there
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))
        cursor.set_key(50)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "initial_value_50")
        self.session.commit_transaction()
        cursor.close()

        # Start a prepared transaction that will be aborted
        session_prepare = self.conn.open_session()
        cursor_prepare = session_prepare.open_cursor(uri)
        session_prepare.begin_transaction()

        # Make updates in the prepared transaction
        for i in range(1, 100):
            cursor_prepare[i] = "prepared_value_" + str(i)

        # Prepare the transaction with timestamp 70
        session_prepare.prepare_transaction('prepare_timestamp=' + self.timestamp_str(70)+',prepared_id=' + self.prepared_id_str(1))

        # Abort the prepared transaction with rollback timestamp 80
        session_prepare.rollback_transaction('rollback_timestamp=' + self.timestamp_str(80))

        # Set table timestamp to be after prepare timestamp, but before rollback timestamp.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(75))

        # Since prepare timestamp is stable but rollback ts is not, we write the prepared update to disk
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        rec_time_window_prepared = stat_cursor[wiredtiger.stat.conn.rec_time_window_prepared][2]
        self.assertEqual(rec_time_window_prepared, 99)

        stat_cursor.close()
        cursor_prepare.close()
        session_prepare.close()
        self.session.close()
