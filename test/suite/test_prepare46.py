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

# test_prepare46.py
# Regression test for a bug in reconciliation where a prepare rollback tombstone was
# incorrectly selected for disk write when the rollback timestamp was not yet stable.
#
# Update chain for the key under test:
#   [rollback_tombstone] -> [aborted_prepare(rollback_ts=50)] -> [committed_value(ts=20)]
#
# When stable_ts < prepare_ts, reconciliation must skip both the tombstone and the aborted
# prepare and select the committed value instead.  Without the fix, prepare_rollback_tombstone
# remained set after skipping the aborted prepared update, causing an assertion failure (in
# diagnostic builds) when reconciliation tried to select the following committed value.

import wiredtiger
import wttest

class test_prepare46(wttest.WiredTigerTestCase):

    conn_config = 'precise_checkpoint=true,preserve_prepared=true'
    uri = 'table:test_prepare46'

    def test_rollback_tombstone_skipped_when_rollback_not_stable(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        # Commit a value for key 1 at ts=20.
        self.session.begin_transaction()
        cursor[1] = 'committed_value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))

        # Prepare an update at ts=30, then roll it back at rollback_ts=50.
        # This creates: [tombstone] -> [aborted_prepare(rollback_ts=50)] -> [committed_value(ts=20)]
        session_prep = self.conn.open_session()
        cursor_prep = session_prep.open_cursor(self.uri)
        session_prep.begin_transaction()
        cursor_prep[1] = 'prepared_value'
        session_prep.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(30) +
            ',prepared_id=' + self.prepared_id_str(1))
        cursor_prep.close()
        session_prep.rollback_transaction('rollback_timestamp=' + self.timestamp_str(50))
        session_prep.close()

        # stable=35 is below rollback_ts=50  rollback not yet stable.
        # Reconciliation must not select the tombstone; committed_value must be preserved.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(35))

        # Without the fix this triggers an assertion failure in diagnostic builds.
        session_evict = self.conn.open_session()
        session_evict.begin_transaction('ignore_prepare=true')
        evict_cursor = session_evict.open_cursor(self.uri, None, 'debug=(release_evict)')
        evict_cursor.set_key(1)
        self.assertEqual(evict_cursor.search(), 0)
        self.assertEqual(evict_cursor.get_value(), 'committed_value')
        evict_cursor.reset()
        evict_cursor.close()
        session_evict.rollback_transaction()
        session_evict.close()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        cursor.set_key(1)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'committed_value')
        self.session.rollback_transaction()

        # Now the rollback is stable; the tombstone can be written safely on the next reconciliation.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(55))
        session_evict2 = self.conn.open_session()
        session_evict2.begin_transaction('ignore_prepare=true')
        evict_cursor2 = session_evict2.open_cursor(self.uri, None, 'debug=(release_evict)')
        evict_cursor2.set_key(1)
        self.assertIn(evict_cursor2.search(), [0, wiredtiger.WT_NOTFOUND])
        evict_cursor2.reset()
        evict_cursor2.close()
        session_evict2.rollback_transaction()
        session_evict2.close()

        # After the tombstone is written, reads at ts=20 must still resolve via the history store.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        cursor.set_key(1)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'committed_value')
        self.session.rollback_transaction()

        cursor.close()
