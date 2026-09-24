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

# test_layered_async_stepdown01.py
#    Write routing and write semantics across a step-down: leader writes, including those above the
#    final checkpoint, land in the live stable table; after the demotion follower writes land in
#    ingest over the frozen stable table, and the merged view drives duplicate-key detection,
#    overwrite=false, modify and reserve.
@disagg_test_class
class test_layered_async_stepdown01(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = \
        'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),precise_checkpoint=true,'
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = f'layered:{test_name}'

    # Leader writes land in stable on either side of the final checkpoint; after the demotion the
    # follower reads them all and its own writes land in ingest.
    def test_write_routing_around_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        before = {'before' + str(i) for i in range(5)}
        window = {'window' + str(i) for i in range(5)}
        after = {'after' + str(i) for i in range(5)}

        self.write_at(self.uri, {k: 'stable' for k in before}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {k: 'window' for k in window}, 30)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 35), set(),
            'leader writes must not be in the ingest table')
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 35), before | window,
            'leader writes must land in the stable table')

        self.demote()
        self.assertEqual(self.read_keys_at(self.uri, 35), before | window)
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(self.uri), 35), before)

        self.write_at(self.uri, {k: 'ingest' for k in after}, 40)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 45), after)
        self.assertEqual(self.read_keys_at(self.uri, 45), before | window | after)

    # Follower update, modify and remove of keys held only by the frozen stable table land in
    # ingest, whether the key predates the final checkpoint or was written above it.
    def test_update_modify_remove_routing_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.write_at(self.uri, {'k1': 'base', 'k2': 'base', 'k3': 'base'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'w1': 'base', 'w2': 'base', 'w3': 'base'}, 25)
        self.demote()

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'updated'
        cursor['w1'] = 'updated'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        self.remove_at(self.uri, ['k2', 'w2'], 31)

        # Modify builds the new value on the frozen stable base and writes the result to ingest.
        self.session.begin_transaction()
        for key in ('k3', 'w3'):
            cursor.set_key(key)
            self.assertEqual(cursor.modify([wiredtiger.Modify('v', 0, 1)]), 0)  # 'base' -> 'vase'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(32))
        cursor.close()

        expected = {'k1': 'updated', 'k3': 'vase', 'w1': 'updated', 'w3': 'vase'}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)

        # All of them landed in ingest, the removes as tombstones shadowing stable.
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40),
            {'k1': 'updated', 'k2': '\x14', 'k3': 'vase',
             'w1': 'updated', 'w2': '\x14', 'w3': 'vase'})
        self.assertEqual(self.read_kvs_at(self.uri, 28),
            {'k1': 'base', 'k2': 'base', 'k3': 'base', 'w1': 'base', 'w2': 'base', 'w3': 'base'})

    # A size-changing modify produces the same value whether the key predates the final checkpoint
    # or was written above it.
    def test_size_changing_modify_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'stable-only': 'abcde'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'window': 'abcde'}, 25)
        self.demote()

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        for key in ('stable-only', 'window'):
            cursor.set_key(key)
            self.assertEqual(cursor.modify([wiredtiger.Modify('X', 1, 0)]), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))
        cursor.close()

        expected = {'stable-only': 'aXbcde', 'window': 'aXbcde'}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), expected)

    # Removing an ingest key during iteration must not lose the frozen stable neighbor in either
    # direction.
    def test_remove_ingest_key_during_iteration(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'a': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'c': 'window'}, 25)
        self.demote()
        self.write_at(self.uri, {'b': 'ingest'}, 30)

        cursor = self.session.open_cursor(self.uri, None, None)

        self.session.begin_transaction()
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'a')
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'b')
        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'c')
        self.session.rollback_transaction()

        cursor.reset()
        self.session.begin_transaction()
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'c')
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'b')
        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'a')
        self.session.rollback_transaction()
        cursor.close()

    # One demotion freezes every table: each serves its own leader writes and takes follower writes
    # into its own ingest.
    def test_multiple_tables_share_step_down(self):
        uri1 = f'layered:{self.test_name}_multi1'
        uri2 = f'layered:{self.test_name}_multi2'
        self.set_global_ts(1, 1)
        self.session.create(uri1, 'key_format=S,value_format=S')
        self.session.create(uri2, 'key_format=S,value_format=S')

        self.write_at(uri1, {'a': 'stable'}, 10)
        self.write_at(uri2, {'b': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(uri1, {'c': 'window'}, 25)
        self.demote()

        self.write_at(uri1, {'e': 'ingest'}, 30)
        self.write_at(uri2, {'d': 'ingest'}, 30)

        self.assertEqual(self.read_keys_at(self.ingest_uri(uri1), 40), {'e'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(uri2), 40), {'d'})
        self.assertEqual(self.read_kvs_at(uri1, 40), {'a': 'stable', 'c': 'window', 'e': 'ingest'})
        self.assertEqual(self.read_kvs_at(uri2, 40), {'b': 'stable', 'd': 'ingest'})

    # A non-overwrite insert of a key in the frozen stable table conflicts even though the write
    # targets ingest.
    def test_duplicate_key_detection_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'dup': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'dupw': 'window'}, 25)
        self.demote()

        cursor = self.session.open_cursor(self.uri, None, "overwrite=false")
        for key in ('dup', 'dupw'):
            self.session.begin_transaction()
            cursor.set_key(key)
            cursor.set_value('again')
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.insert(),
                wiredtiger.wiredtiger_strerror(wiredtiger.WT_DUPLICATE_KEY))
            self.session.rollback_transaction()
        cursor.close()

        # The rejected inserts left the stable values alone and nothing in ingest.
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'dup': 'stable', 'dupw': 'window'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())

    # overwrite=false update and remove consult the merged view; the writes land in ingest.
    def test_overwrite_false_ops_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'k2': 'window'}, 25)
        self.demote()

        cursor = self.session.open_cursor(self.uri, None, "overwrite=false")

        # Update of a key checkpointed in stable: found in the merged view, written to ingest.
        self.session.begin_transaction()
        cursor.set_key('k1')
        cursor.set_value('updated')
        self.assertEqual(cursor.update(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        # Remove of a key only the frozen tree holds: a tombstone routed to ingest.
        self.session.begin_transaction()
        cursor.set_key('k2')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))

        # Update and remove of a missing key fail across the merged view.
        self.session.begin_transaction()
        cursor.set_key('missing')
        cursor.set_value('v')
        self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        cursor.set_key('missing')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'updated'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1', 'k2'})
        self.assertEqual(self.read_kvs_at(self.uri, 28), {'k1': 'base', 'k2': 'window'})

    # A reserve conflicts with concurrent writers and leaves no content behind.
    def test_reserve_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'k2': 'window'}, 25)
        self.demote()

        for key, value in (('k1', 'stable'), ('k2', 'window')):
            cursor = self.session.open_cursor(self.uri, None, None)
            self.session.begin_transaction()
            cursor.set_key(key)
            self.assertEqual(cursor.reserve(), 0)

            # A concurrent writer conflicts with the reservation.
            wsession = self.conn.open_session()
            wcur = wsession.open_cursor(self.uri, None, None)
            wsession.begin_transaction()
            wcur.set_key(key)
            wcur.set_value('other')
            self.expect_conflict_rollback(wcur.update, wsession)
            wsession.rollback_transaction()
            wcur.close()
            wsession.close()

            # The reserve-only commit leaves no content behind.
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
            cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'stable', 'k2': 'window'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())
