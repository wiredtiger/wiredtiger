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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown07.py
#    Cursor lifecycle across the step-down and cross-constituent write conflicts while armed.
@disagg_test_class
class test_layered_async_stepdown07(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:stepdown_lifecycle'

    def expect_rollback(self, func):
        self.assertRaisesException(wiredtiger.WiredTigerError, func,
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))

    # A cursor closed before the step-down and one opened after it serve the same view.
    def test_cursor_close_reopen_within_txn_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's'}, 10)

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        c1 = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(c1['b'], 's')
        c1.close()

        self.arm(20)
        self.complete_step_down(20)

        c2 = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(c2['b'], 's')
        self.assertEqual(c2['d'], 's')
        self.session.commit_transaction()
        c2.close()

    # One cursor handle works for transactions on both sides of the step-down.
    def test_same_cursor_handle_across_step_down_txns(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'b')
        self.session.rollback_transaction()

        self.arm(20)
        self.write_at(self.uri, {'a': 'i', 'z': 'i'}, 30)
        self.complete_step_down(20)

        # Reset at the transaction end, the handle walks the merged view from the start.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        seen = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.assertEqual(seen, ['a', 'b', 'd', 'f', 'z'])
        cursor.set_key('d')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 's')
        self.session.rollback_transaction()
        cursor.close()

    # Duplicating a layered cursor is unsupported, armed or not.
    def test_dup_positioned_cursor_across_arm(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        c1 = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(c1.next(), 0)
        self.assertEqual(c1.get_key(), 'b')

        with self.expectedStderrPattern('unsupported object operation'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(None, c1, None))

        self.arm(20)

        with self.expectedStderrPattern('unsupported object operation'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(None, c1, None))

        # The original cursor survives the rejected duplication.
        self.assertEqual(c1.get_key(), 'b')
        self.assertEqual(c1.next(), 0)
        self.assertEqual(c1.get_key(), 'd')

        self.session.rollback_transaction()
        c1.close()

    # Visibility flips at exactly the cutoff after the completed step-down.
    def test_boundary_reads_at_cutoff(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.write_at(self.uri, {'below': 'v'}, 19)
        self.write_at(self.uri, {'at': 'v'}, 20)
        self.arm(20)
        self.write_at(self.uri, {'above': 'v'}, 21)
        self.complete_step_down(20)

        self.assertEqual(self.read_kvs_at(self.uri, 19), {'below': 'v'})
        self.assertEqual(self.read_kvs_at(self.uri, 20), {'below': 'v', 'at': 'v'})
        self.assertEqual(self.read_kvs_at(self.uri, 21), {'below': 'v', 'at': 'v', 'above': 'v'})

        # Ground truth: the content split exactly at the cutoff.
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 30), {'below', 'at'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 30), {'above'})

    # A straddler's uncommitted delete on stable and an armed remove of the same key land on
    # different constituents with no shared update chain; the conflict probe must still find the
    # collision and roll back, even with no read timestamp.
    def test_armed_remove_conflicts_with_uncommitted_straddler_delete(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['victim'] = 'alive'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # The uncommitted tombstone sits on the stable chain.
        straddler_session = self.conn.open_session()
        straddler_cursor = straddler_session.open_cursor(self.uri, None, None)
        straddler_session.begin_transaction()
        straddler_cursor.set_key('victim')
        self.assertEqual(straddler_cursor.remove(), 0)

        self.arm(20)

        # The armed remove routes to ingest and its snapshot excludes the straddler; the probe
        # must find the uncommitted delete on stable.
        self.session.begin_transaction()
        cursor.set_key('victim')
        self.expect_rollback(cursor.remove)
        self.session.rollback_transaction()

        # The straddler dies at commit.
        self.expect_rollback(lambda: straddler_session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(15)))
        straddler_cursor.close()
        straddler_session.close()

        # With the straddler resolved, the retry commits into ingest.
        self.session.begin_transaction()
        cursor.set_key('victim')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Exactly one delete happened.
        self.assertEqual(self.read_kvs_at(self.uri, 25), {'victim': 'alive'})
        self.assertEqual(self.read_kvs_at(self.uri, 35), {})

    # The same conflict with a read timestamp stays caught.
    def test_armed_remove_conflicts_with_read_timestamp(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['victim'] = 'alive'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        straddler_session = self.conn.open_session()
        straddler_cursor = straddler_session.open_cursor(self.uri, None, None)
        straddler_session.begin_transaction()
        straddler_cursor.set_key('victim')
        self.assertEqual(straddler_cursor.remove(), 0)

        self.arm(20)

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        cursor.set_key('victim')
        self.expect_rollback(cursor.remove)
        self.session.rollback_transaction()
        cursor.close()

        straddler_session.rollback_transaction()
        straddler_cursor.close()
        straddler_session.close()
