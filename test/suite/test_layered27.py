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

import platform
import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wtscenario import make_scenarios

# test_layered27.py
# Test draining the ingest table
@disagg_test_class
class test_layered27(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                 + ''

    sizes = [
        ('small', dict(multiplier=1)),
        ('large', dict(multiplier=100)),
    ]

    disagg_storages = gen_disagg_storages('test_layered27', disagg_only = True)

    scenarios = make_scenarios(disagg_storages, sizes)

    uri = 'layered:test_layered27'

    @property
    def base_config(self):
        return self.extensionsConfig() + self.conn_base_config

    def conn_config(self):
        return self.base_config + 'disaggregated=(role="leader")'

    @property
    def conn_follower_config(self):
        return self.base_config + 'disaggregated=(role="follower")'

    def test_drain_insert_update(self):
        # Create the oplog
        oplog = Oplog()

        # Create the table on leader and tell oplog about it
        self.session.create(self.uri, "key_format=S,value_format=S")
        t = oplog.add_uri(self.uri)

        # Create the follower and create its table
        # To keep this test relatively easy, we're only using a single URI.
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Create some oplog traffic
        oplog.insert(t, 100 * self.multiplier)

        # Apply them to leader WT and checkpoint.
        oplog.apply(self, self.session, 0, 100 * self.multiplier)
        oplog.check(self, self.session, 0, 100 * self.multiplier)

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')

        self.session.checkpoint()     # checkpoint 1

        # Add some more traffic
        oplog.insert(t, 100 * self.multiplier)
        oplog.update(t, 200 * self.multiplier)

        # FIXME-WT-15763: Re-enable once we can abandon changes after stepping down.
        # oplog.apply(self, self.session, 100 * self.multiplier, 300 * self.multiplier)
        # oplog.check(self, self.session, 0, 400 * self.multiplier)

        # On the follower -
        # Apply all the entries to follower
        oplog.apply(self, session_follow, 0, 400 * self.multiplier)

        self.pr(f'OPLOG: {oplog}')
        oplog.check(self, session_follow, 0, 400 * self.multiplier)

        # Then advance the checkpoint and make sure everything is still good
        self.pr('advance checkpoint')
        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, 400 * self.multiplier)

        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Checkpoint after draining the ingest table
        conn_follow.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen the new leader as follower to get rid of the content in the ingest table
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')

        # Ensure everything is in the new checkpoint
        oplog.check(self, session_follow, 0, 400 * self.multiplier)

    def test_drain_remove(self):
        # Create the oplog
        oplog = Oplog()

        # Create the table on leader and tell oplog about it
        self.session.create(self.uri, "key_format=S,value_format=S")
        t = oplog.add_uri(self.uri)

        # Create the follower and create its table
        # To keep this test relatively easy, we're only using a single URI.
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Create some oplog traffic
        oplog.insert(t, 100 * self.multiplier)

        # Apply them to leader WT and checkpoint.
        oplog.apply(self, self.session, 0, 100 * self.multiplier)
        oplog.check(self, self.session, 0, 100 * self.multiplier)

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')

        self.session.checkpoint()     # checkpoint 1

        # Delete some updates
        oplog.remove(t, 100 * self.multiplier)

        # FIXME-WT-15763: Re-enable once we can abandon changes after stepping down.
        # oplog.apply(self, self.session, 100 * self.multiplier, 100 * self.multiplier)
        # oplog.check(self, self.session, 0, 200 * self.multiplier)

        # On the follower -
        # Apply all the entries to follower
        oplog.apply(self, session_follow, 0, 200 * self.multiplier)

        self.pr(f'OPLOG: {oplog}')
        oplog.check(self, session_follow, 0, 200 * self.multiplier)

        # Then advance the checkpoint and make sure everything is still good
        self.pr('advance checkpoint')
        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, 200 * self.multiplier)

        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Checkpoint after draining the ingest table
        conn_follow.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen the new leader as follower to get rid of the content in the ingest table
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')

        # Ensure everything is in the new checkpoint
        oplog.check(self, session_follow, 0, 200 * self.multiplier)

    # This test ensures there are no consecutive tombstones in the update chain
    # when draining the ingest table.
    # See also: WT-15721, WT-16085.
    def test_drain_insert_remove_within_same_transaction(self):
        key = 'key1'
        ts1, ts2, ts3, ts4, ts5 = 10, 20, 30, 40, 50

        # Create the layered table on both leader and follower.
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.session = self.conn.open_session('')
        cursor = self.session.open_cursor(self.uri)

        # 1. Insert the key at T1.
        self.session.begin_transaction()
        cursor[key] = str(ts1)
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts1)}')

        # 2. Delete the key at T2.
        self.session.begin_transaction()
        cursor.set_key(key)
        cursor.remove()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts2)}')

        # 3. Start inserting the key again.
        self.session.begin_transaction()
        cursor[key] = str(ts3)

        # 4. Delete the key inside the same transaction.
        cursor.set_key(key)
        cursor.remove()

        # 5. Commit that transaction at T3.
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts3)}')

        # 6. Insert the key at T4.
        self.session.begin_transaction()
        cursor[key] = str(ts4)
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts4)}')

        # 7. Insert the key again at T5.
        self.session.begin_transaction()
        cursor[key] = str(ts5)
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts5)}')

        cursor.close()

        # 8. Step up: promote the follower connection to leader so ingest state drains.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # 9. Make T5 stable on the stepped-up connection.
        ts5_str = self.timestamp_str(ts5)
        self.conn.set_timestamp(f'stable_timestamp={ts5_str}')

        # 10. Checkpoint to drain the ingest table into the base table.
        self.session.checkpoint()

        # End of test.
        self.conn.close()

    def test_tombstone_only_chain_no_on_disk_value(self):
        """
        Regression reproducer for the original WT-17354 crash.
        The tombstone must NOT be globally visible when reconciliation fires —
        oldest_timestamp is deliberately held below ts_delete.
        """
        key = 'key1'
        ts_insert = 10
        ts_delete = 20
        ingest_uri = 'file:test_layered27.wt_ingest'

        self.session.create(self.uri, "key_format=S,value_format=S")

        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Step 1: Insert via leader. Value lands in stable btree only.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[key] = 'value'
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(ts_insert)}')
        cursor.close()
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(ts_insert)}')
        self.session.checkpoint()

        # Step 2: Follower picks up the checkpoint.
        self.disagg_advance_checkpoint(conn_follow)

        # Step 3: Delete the key on the follower.
        # Ingest btree now has a tombstone-only chain; no on-disk value.
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction()
        cursor_follow.set_key(key)
        self.assertEqual(cursor_follow.remove(), 0)
        session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(ts_delete)}')
        cursor_follow.close()

        # Step 4: Trigger reconciliation.
        # oldest_timestamp is NOT advanced past ts_delete — tombstone is
        # not globally visible. The original assertion fires here without
        # the fix; with the fix, this should complete cleanly.
        evict_cursor = session_follow.open_cursor(
            ingest_uri, None, 'debug=(release_evict)')
        session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(ts_delete)}')
        evict_cursor.set_key(key)
        ret = evict_cursor.search()
        self.assertTrue(ret == 0 or ret == wiredtiger.WT_NOTFOUND)
        evict_cursor.reset()
        session_follow.rollback_transaction()
        evict_cursor.close()

        # After the fix: deletion should be visible at ts_delete.
        c = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(ts_delete)}')
        c.set_key(key)
        self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND,
            'key should be absent at ts_delete after fix')
        session_follow.rollback_transaction()
        c.close()

        session_follow.close()
        conn_follow.close()

    def test_tombstone_only_gc_cycle(self):
        # Regression test for WT-17354. The bug is not load-sensitive; run once.
        if self.multiplier != 1:
            return

        # When a key is inserted via the leader and then deleted on the follower,
        # the ingest btree's insert list holds a tombstone with no preceding on-disk cell
        # GC reconciliation of the ingest btree used to crash in
        # __rec_fill_tw_from_upd_select with "No on-disk value is found".
        # The fix writes an empty cell so the tombstone survives
        # re-instantiation. This test verifies that fix:
        #   1. Insert via leader -> key in stable btree only
        #   2. Delete on follower -> tombstone-only chain in ingest insert list
        #   3. First GC reconciliation pass (forced eviction  the crash site)
        #   4. Verify deletion survives re-instantiation
        #   5. Second GC reconciliation pass (delete cell from pass 1 as input)
        #   6. Verify deletion survives second re-instantiation
        #   7. Step-up / drain and final verification

        key = 'key1'
        ts_insert = 10
        ts_delete = 20

        self.session.create(self.uri, "key_format=S,value_format=S")

        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Step 1: insert via leader, checkpoint, advance follower.
        # After this the key's value lives in the stable btree only.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[key] = 'value'
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts_insert)}')
        cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts_insert)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Step 2: delete the key on the follower.
        # The ingest btree insert list now contains a single tombstone
        # there is no on-disk value in the ingest btree.
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction()
        cursor_follow.set_key(key)
        self.assertEqual(cursor_follow.remove(), 0)
        session_follow.commit_transaction(f'commit_timestamp={self.timestamp_str(ts_delete)}')
        cursor_follow.close()

        def assert_present(label):
            c = session_follow.open_cursor(self.uri)
            session_follow.begin_transaction(
                f'read_timestamp={self.timestamp_str(ts_insert)}')
            c.set_key(key)
            self.assertEqual(c.search(), 0,
                f'{label}: key should be present but was not found')
            session_follow.rollback_transaction()
            c.close()

        def assert_deleted(label):
            c = session_follow.open_cursor(self.uri)
            session_follow.begin_transaction(
                f'read_timestamp={self.timestamp_str(ts_delete)}')
            c.set_key(key)
            self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND,
                f'{label}: key should be absent but was found')
            session_follow.rollback_transaction()
            c.close()

        assert_present('before checkpoint')
        assert_deleted('before checkpoint')

        # Steps 3-4: two GC reconciliation passes driven by checkpoint.
        # Checkpoint reconciles the ingest btree with WT_REC_CHECKPOINT|WT_REC_HS
        # (no scrub), so the tombstone MUST be written to disk.
        #
        # rec_visibility.c forces the tombstone to be selected (upd_select->upd =
        # tombstone) even when the checkpoint stable_timestamp is below ts_delete,
        # because the ingest btree has no on-disk value for the key.
        #
        # Without the fix: __rec_row_leaf_insert writes the key cell but no value
        # cell (val->len stays 0, __rec_row_zero_len returns false for a tombstone
        # time window).  entries=2 but only 1 physical cell is on the page.
        # __wt_verify_dsk_image (HAVE_DIAGNOSTIC) detects the mismatch and aborts.
        #
        # With the fix: __wti_rec_cell_build_val writes a proper zero-length delete
        # cell anchored by the tombstone time window.  verify passes.
        for pass_num in (1, 2):
            session_follow.checkpoint()
            assert_present(f'after checkpoint pass {pass_num}')
            assert_deleted(f'after checkpoint pass {pass_num}')

        # Step 7: step up, drain ingest table into stable btree, verify.
        # Use ts_delete + 1 as the stable timestamp so the tombstone's durable
        # timestamp (ts_delete) is unambiguously within the drain window.
        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')
        conn_follow.set_timestamp(f'stable_timestamp={self.timestamp_str(ts_delete + 1)}')
        session_follow.checkpoint()

        assert_present('after step-up and drain')
        assert_deleted('after step-up and drain')

        session_follow.close()
        conn_follow.close()

    def test_drain_remove_insert(self):
        # Create the oplog
        oplog = Oplog()

        # Create the table on leader and tell oplog about it
        self.session.create(self.uri, "key_format=S,value_format=S")
        t = oplog.add_uri(self.uri)

        # Create the follower and create its table
        # To keep this test relatively easy, we're only using a single URI.
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Create some oplog traffic
        oplog.insert(t, 100 * self.multiplier)

        # Apply them to leader WT and checkpoint.
        oplog.apply(self, self.session, 0, 100 * self.multiplier)
        oplog.check(self, self.session, 0, 100 * self.multiplier)

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')

        self.session.checkpoint()     # checkpoint 1

        # Delete some updates
        oplog.remove(t, 100 * self.multiplier)
        oplog.insert(t, 100 * self.multiplier, 0)

        # FIXME-WT-15763: Re-enable once we can abandon changes after stepping down.
        # oplog.apply(self, self.session, 100 * self.multiplier, 200 * self.multiplier)
        # oplog.check(self, self.session, 0, 300 * self.multiplier)

        # On the follower -
        # Apply all the entries to follower
        oplog.apply(self, session_follow, 0, 300 * self.multiplier)

        self.pr(f'OPLOG: {oplog}')
        oplog.check(self, session_follow, 0, 300 * self.multiplier)

        # Then advance the checkpoint and make sure everything is still good
        self.pr('advance checkpoint')
        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, 300 * self.multiplier)

        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Checkpoint after draining the ingest table
        conn_follow.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen the new leader as follower to get rid of the content in the ingest table
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')

        # Ensure everything is in the new checkpoint
        oplog.check(self, session_follow, 0, 300 * self.multiplier)
