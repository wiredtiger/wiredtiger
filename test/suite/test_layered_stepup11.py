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

# test_layered_stepup11.py
#   Tests for the parallel ingest-table drain, which subdivides a table's key space into
#   ranges that drain concurrently on step-up. Exercises concurrent multi-table
#   drain, empty-table skip, single-threaded drain, prepared-transaction redirect
#   (single key, across multiple ranges, and multiple keys in one range), standalone
#   ingest-tombstone eviction, updates to existing stable keys, the no-subdivision
#   path, and follower range-truncate replay.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup11(wttest.WiredTigerTestCase):
    conn_base_config = (
        ',create,statistics=(all),'
        'precise_checkpoint=true,'
        'preserve_prepared=true,'
    )

    disagg_storages = gen_disagg_storages('test_layered_stepup11', disagg_only=True)

    @property
    def base_config(self):
        return self.extensionsConfig() + self.conn_base_config

    def conn_config(self):
        return self.base_config + 'disaggregated=(role="leader")'

    @property
    def conn_follower_config(self):
        return self.base_config + 'disaggregated=(role="follower")'

    # The drain subdivides a table only above MIN_RANGE_SIZE=1000 records. multiplier
    # scales the scenarios' record counts to straddle that threshold: multiplier=1
    # stays below it (no subdivision), multiplier=10 crosses it so larger tables split.

    sizes = [
        ('small', dict(multiplier=1)),
        ('large', dict(multiplier=10)),
    ]

    resolve = [
        ('commit',   dict(do_commit=True)),
        ('rollback', dict(do_commit=False)),
    ]

    scenarios = make_scenarios(disagg_storages, sizes, resolve)

    def test_drain_multiple_tables(self):
        """
        Drain three tables of different sizes concurrently on step-up; verify every
        table's data survives the follower->leader transition.
        """
        uri_a = 'layered:test_layered_stepup11_a'
        uri_b = 'layered:test_layered_stepup11_b'
        uri_c = 'layered:test_layered_stepup11_c'

        oplog = Oplog()
        t_a = oplog.add_uri(uri_a)
        t_b = oplog.add_uri(uri_b)
        t_c = oplog.add_uri(uri_c)

        # First batch: leader applies these entries and checkpoints.
        # With multiplier=10 the third table's second batch will be > MIN_RANGE_SIZE.
        n_a1 = 50  * self.multiplier
        n_b1 = 100 * self.multiplier
        n_c1 = 200 * self.multiplier
        oplog.insert(t_a, n_a1)
        oplog.insert(t_b, n_b1)
        oplog.insert(t_c, n_c1)

        for uri in (uri_a, uri_b, uri_c):
            self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n_a1 + n_b1 + n_c1)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Second batch: only the follower applies these (they go into the ingest).
        # With multiplier=10: uri_c gets 3000 ingest entries -> subdivided.
        n_a2 = 50  * self.multiplier
        n_b2 = 100 * self.multiplier
        n_c2 = 300 * self.multiplier
        oplog.insert(t_a, n_a2)
        oplog.insert(t_b, n_b2)
        oplog.insert(t_c, n_c2)
        total = n_a1 + n_b1 + n_c1 + n_a2 + n_b2 + n_c2

        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        for uri in (uri_a, uri_b, uri_c):
            session_follow.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, session_follow, 0, total)
        oplog.check(self, session_follow, 0, total)

        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, total)

        # Step down leader, step up follower (drain runs on all three tables).
        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen as follower and verify all three tables.
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, total)

    def test_drain_empty_ingest_tables(self):
        """Empty ingest table is skipped by the drain; stable data survives step-up."""
        uri = 'layered:test_layered_stepup11_empty'
        n = 100

        oplog = Oplog()
        t = oplog.add_uri(uri)
        oplog.insert(t, n)

        # Leader: write and checkpoint so records are in stable.
        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Follower: create the table and pick up the leader checkpoint, but
        # write nothing to the follower's ingest.
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(uri, 'key_format=S,value_format=S')
        self.disagg_advance_checkpoint(conn_follow)

        # Step down leader, step up follower.
        # The ingest table is empty so drain is a no-op.
        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen as follower; the new-leader checkpoint contains the stable
        # data and the follower picks it up automatically on open.
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, n)

    def test_drain_single_thread(self):
        """
        drain_threads=1 forces a single-worker full-table drain. The ingest table holds
        more than MIN_RANGE_SIZE=1000 records, so the default 8 threads would subdivide
        it; with 1 thread it gets a single full-table work item instead.
        """
        uri = 'layered:test_layered_stepup11_single'

        oplog = Oplog()
        t = oplog.add_uri(uri)

        # First batch on leader -- establishes last_checkpoint_timestamp so that
        # the follower's ingest entries (second batch) will pass the drain filter.
        n1 = 50 * self.multiplier
        oplog.insert(t, n1)

        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n1)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Second batch goes into the follower's ingest.
        # With multiplier=10: 2000 ingest records exceed 2*MIN_RANGE_SIZE, so
        # the default 8 threads would subdivide -- but drain_threads=1 won't.
        n2 = 200 * self.multiplier
        oplog.insert(t, n2)
        total = n1 + n2

        # drain_threads must be set at connection-open time; it is not re-read
        # during reconfigure (parsed in the init-only section of conn_layered.c).
        single_thread_config = (
            self.extensionsConfig() + self.conn_base_config
            + 'disaggregated=(role="follower",drain_threads=1)'
        )
        conn_follow = self.wiredtiger_open('follower', single_thread_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(uri, 'key_format=S,value_format=S')

        oplog.apply(self, session_follow, 0, total)
        oplog.check(self, session_follow, 0, total)

        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, total)

        self.conn.close('debug=(skip_checkpoint=true)')

        # Step up -- drain runs with a single worker thread.
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, total)

    # Helpers for the prepared-transaction tests. Use integer keys so key ordering
    # is numeric and range boundaries are predictable.

    def _insert_range(self, session, cursor, start, stop, ts_start):
        """Insert integer keys [start, stop) each in its own transaction."""
        ts = ts_start
        for k in range(start, stop):
            session.begin_transaction()
            cursor.set_key(k)
            cursor.set_value(f'v{k}')
            cursor.insert()
            session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
            ts += 1
        return ts  # next available timestamp

    def _verify_range(self, session, cursor, start, stop, ts_read, expect_present=True):
        """Assert keys [start, stop) are present (or absent) at the given read ts."""
        session.begin_transaction(f'read_timestamp={self.timestamp_str(ts_read)}')
        for k in range(start, stop):
            cursor.set_key(k)
            ret = cursor.search()
            if expect_present:
                self.assertEqual(ret, 0, f'key {k} missing')
                self.assertEqual(cursor.get_value(), f'v{k}')
            else:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f'key {k} unexpectedly found')
        session.rollback_transaction()

    def _verify_key(self, session, cursor, key, ts_read, expect_value):
        """Assert a single key equals expect_value (or is absent if None) at ts_read."""
        session.begin_transaction(f'read_timestamp={self.timestamp_str(ts_read)}')
        cursor.set_key(key)
        ret = cursor.search()
        if expect_value is None:
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f'key {key} unexpectedly found')
        else:
            self.assertEqual(ret, 0, f'key {key} missing')
            self.assertEqual(cursor.get_value(), expect_value)
        session.rollback_transaction()

    def test_drain_prepared_transaction(self):
        """
        Prepared transaction (single key) redirected from ingest to stable during drain.

        Timeline:
          ts 1..50   : leader writes keys 1..50 to stable (pre-drain baseline).
          ts 51      : stable_timestamp=50, checkpoint.
          [reconfigure to follower]
          ts 52..101 : follower writes keys 101..150 to ingest (committed).
          ts 200     : prepare_session prepares key 999 (prepare_timestamp=200).
          stable_timestamp=200.
          [reconfigure to leader -- drain runs]
          ts 300     : commit or rollback the prepared transaction.
          stable_timestamp=300, checkpoint.
        """
        uri = 'layered:test_layered_stepup11_prep'
        self.session.create(uri, 'key_format=i,value_format=S')

        cursor = self.session.open_cursor(uri)

        # Leader writes keys 1..50 at timestamps 1..50.
        ts = self._insert_range(self.session, cursor, 1, 51, ts_start=1)
        # ts == 51 now
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(50)}')
        self.session.checkpoint()
        cursor.close()

        # Switch to follower role.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Follower writes keys 101..150 (committed) into the ingest.
        ts = self._insert_range(follower_session, follower_cursor, 101, 151, ts_start=ts)
        follower_cursor.close()

        # Prepare key 999 in a separate session (leaves prepared update in ingest).
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        prepare_cursor.set_key(999)
        prepare_cursor.set_value('prepared_value')
        prepare_cursor.insert()
        prepare_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(200)},'
            f'prepared_id={self.prepared_id_str(1)}')
        prepare_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(200)}')

        # Step up -- drain runs, __layered_fix_prepared_transaction redirects
        # the prepared session's op->btree from ingest -> stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Resolve the prepared transaction after drain.
        if self.do_commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(300)},'
                f'durable_timestamp={self.timestamp_str(300)}')
        else:
            prepare_session.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(300)}')
        prepare_session.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(300)}')
        follower_session.checkpoint()

        # Verify.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        # Keys 1..50: from original stable (always present).
        self._verify_range(read_session, read_cursor, 1, 51,
                           ts_read=50, expect_present=True)

        # Keys 101..150: drained from ingest (committed at ts 51..100).
        self._verify_range(read_session, read_cursor, 101, 151,
                           ts_read=200, expect_present=True)

        # Key 999: committed value if do_commit, absent if rollback.
        expected_999 = 'prepared_value' if self.do_commit else None
        self._verify_key(read_session, read_cursor, 999,
                         ts_read=300, expect_value=expected_999)

        read_cursor.close()
        read_session.close()

    def test_drain_prepared_transaction_multi_range(self):
        """
        Prepared transaction spanning multiple drain ranges.

        Inserts 10 000 committed keys so the table is subdivided into multiple
        ranges (MIN_RANGE_SIZE=1000, default 8 threads -> up to 8 ranges).
        Three prepared keys are placed at the start, middle, and end of the
        key space to guarantee they fall in different drain ranges.
        """
        uri = 'layered:test_layered_stepup11_prep_multi'
        self.session.create(uri, 'key_format=i,value_format=S')

        # Write one baseline key as leader to establish last_checkpoint_timestamp=1.
        # All follower writes start at ts=2 so they satisfy durable_start_ts > 1 and drain.
        baseline_cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        baseline_cursor.set_key(0)
        baseline_cursor.set_value('baseline')
        baseline_cursor.insert()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        baseline_cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

        # Switch to follower; all writes go into the ingest.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Insert 10 000 committed keys at ts 2..10001.
        # Use non-overlapping key space around the three prepared keys.
        committed_keys = list(range(1, 10001))
        # Reserve keys 500, 5000, 9500 for the prepared transaction.
        prepared_keys = {500, 5000, 9500}
        ts = 2  # start above last_checkpoint_timestamp=1 so entries drain
        for k in committed_keys:
            if k in prepared_keys:
                continue
            follower_session.begin_transaction()
            follower_cursor.set_key(k)
            follower_cursor.set_value(f'v{k}')
            follower_cursor.insert()
            follower_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(ts)}')
            ts += 1
        follower_cursor.close()

        # Prepare a transaction that updates the three spread-out keys.
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        for k in sorted(prepared_keys):
            prepare_cursor.set_key(k)
            prepare_cursor.set_value(f'prep_{k}')
            prepare_cursor.insert()
        prepare_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(20000)},'
            f'prepared_id={self.prepared_id_str(1)}')
        prepare_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(20000)}')

        # Step up -- drain subdivides the table; each range worker processes
        # its slice, and __layered_fix_prepared_transaction is called once
        # per prepared key by whichever range worker owns that key.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Resolve prepared transaction after drain.
        if self.do_commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(30000)},'
                f'durable_timestamp={self.timestamp_str(30000)}')
        else:
            prepare_session.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(30000)}')
        prepare_session.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(30000)}')
        follower_session.checkpoint()

        # Verify committed keys are all present.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        read_session.begin_transaction(
            f'read_timestamp={self.timestamp_str(20000)}')
        for k in committed_keys:
            if k in prepared_keys:
                continue
            read_cursor.set_key(k)
            self.assertEqual(read_cursor.search(), 0, f'committed key {k} missing')
            self.assertEqual(read_cursor.get_value(), f'v{k}')
        read_session.rollback_transaction()

        # Verify the three prepared keys.
        for k in sorted(prepared_keys):
            expected = f'prep_{k}' if self.do_commit else None
            self._verify_key(read_session, read_cursor, k,
                             ts_read=30000, expect_value=expected)

        read_cursor.close()
        read_session.close()

    def test_drain_standalone_ingest_tombstone(self):
        """
        Standalone ingest-tombstone eviction during drain.

        A "standalone" tombstone arises when a document was inserted before oplog
        application began on this node -- so the document's insert lives only in
        the stable btree -- and is subsequently deleted on the follower.  The
        ingest btree then holds a tombstone with NO backing on-disk value.

        This exercises the guard added in rec_visibility.c that allows the
        reconciler to evict such a page without asserting "No on-disk value is
        found".  It also verifies that after step-up and drain the delete is
        reflected in the stable table.

        Timeline:
          ts=10 : leader inserts 'key_to_delete' -> stable btree
          stable_timestamp=10, checkpoint
          [reconfigure to follower]
          ts=20 : follower deletes 'key_to_delete' -> tombstone in ingest only
          ts=21 : follower inserts 'key_sentinel'  -> ingest (same page)
          force eviction of the ingest page          -> exercises rec_visibility.c fix
          [reconfigure to leader -- drain runs]
          verify 'key_to_delete' absent, 'key_sentinel' present
        """
        uri = 'layered:test_layered_stepup11_tombstone'
        ingest_uri = 'file:test_layered_stepup11_tombstone.wt_ingest'

        # Leader: insert key_to_delete so it lives in the stable btree only.
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor.set_key('key_to_delete')
        cursor.set_value('original_value')
        cursor.insert()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        # Reconfigure to follower -- subsequent writes go to the ingest btree.
        self.conn.reconfigure('disaggregated=(role="follower")')

        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Delete key_to_delete: tombstone lands in ingest with no backing insert
        # in the ingest btree (the only insert is in stable from above).
        follower_session.begin_transaction()
        follower_cursor.set_key('key_to_delete')
        follower_cursor.remove()
        follower_session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')

        # Insert a sentinel key so the ingest page has a non-tombstone update
        # that an eviction cursor can search for to position on the same page.
        follower_session.begin_transaction()
        follower_cursor.set_key('key_sentinel')
        follower_cursor.set_value('sentinel_value')
        follower_cursor.insert()
        follower_session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(21)}')
        follower_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(21)}')

        # Force eviction of the ingest btree page.  The page holds a tombstone
        # for key_to_delete whose backing value lives only in the stable btree,
        # exercising drain of a standalone ingest tombstone (a follower delete of
        # a stable-only key).
        evict_session = self.conn.open_session('debug=(release_evict_page)')
        evict_cursor = evict_session.open_cursor(ingest_uri)
        evict_cursor.set_key('key_sentinel')
        evict_cursor.search()  # positions on the page that also holds the tombstone
        evict_cursor.close()   # triggers eviction of the page
        evict_session.close()

        # Step up -- drain copies both the tombstone and the sentinel to stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        follower_session.checkpoint()

        # Verify: key_to_delete absent (tombstone drained), key_sentinel present.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        read_session.begin_transaction(f'read_timestamp={self.timestamp_str(21)}')

        read_cursor.set_key('key_to_delete')
        self.assertEqual(read_cursor.search(), wiredtiger.WT_NOTFOUND,
                         'key_to_delete should be absent after tombstone drain')

        read_cursor.set_key('key_sentinel')
        self.assertEqual(read_cursor.search(), 0,
                         'key_sentinel should be present after drain')
        self.assertEqual(read_cursor.get_value(), 'sentinel_value')

        read_session.rollback_transaction()
        read_cursor.close()
        read_session.close()

    def test_drain_update_existing_stable_key(self):
        """
        Verify that ingest-btree updates to keys that already exist in the stable
        btree are correctly drained into stable after follower step-up.  The test
        also inserts genuinely new keys in the same follower batch to exercise the
        mixed (update + insert) drain path.

        Timeline:
          n_stable keys are written as leader -> stable btree, then checkpointed.
          [reconfigure to follower]
          First n_update of those keys are overwritten (oplog.update) in ingest.
          n_fresh brand-new keys are appended (oplog.insert) in ingest.
          [reconfigure to leader -> drain runs]
          stable_timestamp advanced, checkpoint taken.
          [connection closed and reopened as follower]
          oplog.check verifies every oplog entry (original inserts, updates, fresh
          inserts) against the new leader checkpoint.
        """
        uri = 'layered:test_layered_stepup11_update'

        oplog = Oplog()
        t = oplog.add_uri(uri)

        n_stable = 50 * self.multiplier
        n_update = 20 * self.multiplier
        n_fresh  = 20 * self.multiplier

        # Leader writes n_stable keys and checkpoints them.
        oplog.insert(t, n_stable)

        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n_stable)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Reconfigure as follower; the updates and fresh inserts below land in ingest.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')

        # Overwrite the first n_update stable keys; their updates land in ingest.
        oplog.update(t, n_update)
        # Insert n_fresh brand-new keys; these also land in ingest.
        oplog.insert(t, n_fresh)

        total = n_stable + n_update + n_fresh

        # Apply the follower's batch (the n_update + n_fresh new oplog entries).
        oplog.apply(self, follower_session, n_stable, n_update + n_fresh)

        # Step up; drain moves the ingest entries into stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        follower_session.checkpoint()

        # Close everything, reopen as follower, and verify.
        follower_session.close()
        self.conn.close('debug=(skip_checkpoint=true)')
        self.conn = None

        verify_conn = self.wiredtiger_open(self.home, self.conn_follower_config)
        verify_session = verify_conn.open_session('')

        oplog.check(self, verify_session, 0, total)

        verify_session.close()
        verify_conn.close()

    def test_drain_multiple_prepared_same_range(self):
        """
        Verify that multiple prepared-transaction keys falling in the same
        drain range are all correctly redirected (stable-btree pointer fixed)
        by the single range-0 worker.

        Key layout (string keys, lexicographic order):
          Baseline  (leader, ts=1):        "000" -> "baseline"
          Committed drainable (follower):  "300"@ts=10, "500"@ts=11,
                                           "700"@ts=12, "900"@ts=13
          Prepared  (prepared_id=1):       "100", "150", "200"
                                           (all < "500", all in range 0)

        With 4 drainable committed keys and drain_threads=8 the planner
        produces 2 split points ("500", "700").  All three prepared keys
        are below "500" so they land together in range 0.  Committing or
        rolling back the prepared transaction is governed by self.do_commit
        (True/False), which is already parameterised by the class scenarios.
        """
        uri = 'layered:test_layered_stepup11_prep_multi_range'
        self.session.create(uri, 'key_format=S,value_format=S')

        # Leader baseline: insert "000"="baseline" at ts=1 and checkpoint, establishing
        # last_checkpoint_timestamp=1. All subsequent follower writes use ts > 1 so they
        # satisfy the drain filter (durable_start_ts > 1).
        baseline_cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        baseline_cursor.set_key('000')
        baseline_cursor.set_value('baseline')
        baseline_cursor.insert()
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(1)}')
        baseline_cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

        self.conn.reconfigure('disaggregated=(role="follower")')

        # Follower committed writes: four keys at explicit timestamps (not via oplog) to
        # deterministically control the sample set and therefore the range split points.
        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        committed = [
            ('300', 'v300', 10),
            ('500', 'v500', 11),
            ('700', 'v700', 12),
            ('900', 'v900', 13),
        ]
        for key, val, ts in committed:
            follower_session.begin_transaction()
            follower_cursor.set_key(key)
            follower_cursor.set_value(val)
            follower_cursor.insert()
            follower_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(ts)}')
        follower_cursor.close()

        # All three prepared keys are inserted in a single transaction before
        # prepare_transaction is called, so they share prepared_id=1 and all land in
        # range 0 together during drain.
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        for key, val in [('100', 'prep100'), ('150', 'prep150'), ('200', 'prep200')]:
            prepare_cursor.set_key(key)
            prepare_cursor.set_value(val)
            prepare_cursor.insert()
        prepare_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(200)},'
            f'prepared_id={self.prepared_id_str(1)}')
        prepare_cursor.close()

        # Advance stable_timestamp to cover the prepare timestamp.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(200)}')

        # Step up to leader; drain runs. The range-0 worker owns [unbounded, "500") and
        # encounters keys "100", "150", "200" in that range, calling
        # __layered_fix_prepared_transaction for each to redirect the prepared session's
        # btree pointer from ingest to stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Resolve the prepared transaction after drain completes.
        if self.do_commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(300)},'
                f'durable_timestamp={self.timestamp_str(300)}')
        else:
            prepare_session.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(300)}')

        prepare_session.close()

        # Advance stable_timestamp and checkpoint.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(300)}')
        follower_session.checkpoint()

        # Verify.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        # Baseline key must always be present (written as leader, never drained).
        self._verify_key(read_session, read_cursor, '000',
                         ts_read=1, expect_value='baseline')

        # Four committed keys must be visible after drain (drained from ingest).
        for key, val, _ in committed:
            self._verify_key(read_session, read_cursor, key,
                             ts_read=200, expect_value=val)

        # Three prepared keys: present with correct values on commit, absent on rollback.
        for key, val in [('100', 'prep100'), ('150', 'prep150'), ('200', 'prep200')]:
            expected = val if self.do_commit else None
            self._verify_key(read_session, read_cursor, key,
                             ts_read=300, expect_value=expected)

        read_cursor.close()
        read_session.close()

    def test_drain_tiny_ingest(self):
        """
        A single drainable key in the ingest exercises the no-split path
        through the parallel-drain machinery.  With any drain_threads > 1
        n_splits = n_collected / 2 = 0 so the table receives one unbounded
        work item, the same code path as single-thread mode but reached via
        the sampling gate rather than the thread-count gate.

        Uses the single-connection follower->leader pattern; no
        disagg_advance_checkpoint is needed.
        """
        uri = 'layered:test_layered_stepup11_tiny'
        n_stable = 10

        oplog = Oplog()
        t = oplog.add_uri(uri)

        # Leader: write a small stable baseline and checkpoint to set
        # last_checkpoint_timestamp, so the single ingest key below will
        # have durable_start_ts > last_checkpoint_timestamp and be drained.
        oplog.insert(t, n_stable)
        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n_stable)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Step down; subsequent write goes to the ingest btree.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')

        # Insert exactly 1 key (multiplier NOT used  smallness is intentional).
        oplog.insert(t, 1)
        oplog.apply(self, follower_session, n_stable, 1)

        # Step up; drain creates one unbounded work item and moves the key.
        self.conn.reconfigure('disaggregated=(role="leader")')

        total = n_stable + 1
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        follower_session.checkpoint()
        follower_session.close()

        # Verify on the existing leader connection  no reopen needed.
        verify_session = self.conn.open_session('')
        oplog.check(self, verify_session, 0, total)
        verify_session.close()

    def test_drain_range_truncate_stable_only_keys(self):
        """
        Verify that a follower range truncate covering keys that exist only in the
        stable btree (no ingest btree counterpart) is correctly replayed against
        stable during drain, leaving those keys absent after step-up.
        """
        uri = 'layered:test_layered_stepup11_trunc_stable'

        # --- Leader phase: write three keys into stable. ---
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for k in ('k1', 'k2', 'k3'):
            self.session.begin_transaction()
            cursor.set_key(k)
            cursor.set_value(f'v_{k}')
            cursor.insert()
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}'
                                f',oldest_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

        # --- Step down to follower. k1..k3 are now stable-only. ---
        self.conn.reconfigure('disaggregated=(role="follower")')

        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Range truncate k1..k3.  Because none of these keys are in the ingest
        # btree, __clayered_range_truncate only adds a WT_TRUNCATE list entry
        # and writes no tombstone into the ingest btree.
        c_start = follower_session.open_cursor(uri)
        c_stop  = follower_session.open_cursor(uri)
        c_start.set_key('k1')
        c_stop.set_key('k3')
        follower_session.begin_transaction()
        follower_session.truncate(None, c_start, c_stop, None)
        follower_session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        c_start.close()
        c_stop.close()

        # Also insert a fresh key k4 (only in ingest) to verify normal drain.
        follower_session.begin_transaction()
        follower_cursor.set_key('k4')
        follower_cursor.set_value('v_k4')
        follower_cursor.insert()
        follower_session.commit_transaction(f'commit_timestamp={self.timestamp_str(21)}')
        follower_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(21)}')

        # --- Step up: drain must replay the WT_TRUNCATE against stable. ---
        self.conn.reconfigure('disaggregated=(role="leader")')
        follower_session.checkpoint()
        follower_session.close()

        # --- Verify. ---
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)
        read_session.begin_transaction(f'read_timestamp={self.timestamp_str(21)}')

        for k in ('k1', 'k2', 'k3'):
            read_cursor.set_key(k)
            self.assertEqual(read_cursor.search(), wiredtiger.WT_NOTFOUND,
                f'{k} should be absent after truncate drain')

        read_cursor.set_key('k4')
        self.assertEqual(read_cursor.search(), 0, 'k4 should be present after drain')
        self.assertEqual(read_cursor.get_value(), 'v_k4')

        read_session.rollback_transaction()
        read_cursor.close()
        read_session.close()


    def test_drain_range_truncate_ingest_and_stable_keys(self):
        """
        Verify that __layered_apply_truncate_to_stable skips keys that were
        already tombstoned by the ingest drain (keys present in both the ingest
        and stable btrees), preventing duplicate tombstones.

        Scenario:
          T=10  (leader)   write k1, k2  checkpoint  both stable-only
          T=20  (follower) write k2 into ingest (k2 now in both ingest+stable)
          T=30  (follower) truncate [k1..k2]:
                             k1 is stable-only  WT_TRUNCATE entry only (no ingest tombstone)
                             k2 is in ingest    tombstone at T=30 written to ingest +
                                                 WT_TRUNCATE entry queued
          step-up:
            ingest drain copies k2 tombstone@T=30 to stable
            __layered_apply_truncate_to_stable opens cursors at read_timestamp=T_trunc:
              k2 appears deleted (ingest tombstone)  skipped (no duplicate tombstone)
              k1 still visible                       tombstoned correctly
        """
        uri = 'layered:test_layered_stepup11_trunc_ingest_stable_keys'

        # --- Leader phase ---
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for k, v in (('k1', 'v1_orig'), ('k2', 'v2_orig')):
            self.session.begin_transaction()
            cursor.set_key(k)
            cursor.set_value(v)
            cursor.insert()
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}'
                                f',oldest_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

        # --- Step down ---
        self.conn.reconfigure('disaggregated=(role="follower")')

        fsession = self.conn.open_session('')

        # T=20: write k2 into ingest  k2 is now in both stable and ingest.
        fcursor = fsession.open_cursor(uri)
        fsession.begin_transaction()
        fcursor.set_key('k2')
        fcursor.set_value('v2_ingest')
        fcursor.update()
        fsession.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        fcursor.close()

        # T=30: truncate [k1..k2].
        #   k1 stable-only  WT_TRUNCATE entry only.
        #   k2 in ingest    tombstone written to ingest at T=30 + WT_TRUNCATE entry.
        c_start = fsession.open_cursor(uri)
        c_stop  = fsession.open_cursor(uri)
        c_start.set_key('k1')
        c_stop.set_key('k2')
        fsession.begin_transaction()
        fsession.truncate(None, c_start, c_stop, None)
        fsession.commit_transaction(f'commit_timestamp={self.timestamp_str(30)}')
        c_start.close()
        c_stop.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(30)}')

        # --- Step up and drain ---
        self.conn.reconfigure('disaggregated=(role="leader")')
        fsession.checkpoint()
        fsession.close()

        # --- Verify MVCC chain ---
        rsession = self.conn.open_session('')

        def read_key(ts, key):
            rsession.begin_transaction(f'read_timestamp={self.timestamp_str(ts)}')
            rc = rsession.open_cursor(uri)
            rc.set_key(key)
            ret = rc.search()
            val = rc.get_value() if ret == 0 else None
            rsession.rollback_transaction()
            rc.close()
            return ret, val

        # k1: stable-only; tombstoned by __layered_apply_truncate_to_stable at T=30.
        self.assertEqual(read_key(29, 'k1'), (0, 'v1_orig'), 'k1 visible before truncate')
        self.assertEqual(read_key(30, 'k1')[0], wiredtiger.WT_NOTFOUND, 'k1 absent at truncate ts')

        # k2: tombstoned by ingest drain at T=30; __layered_apply_truncate_to_stable
        # must skip it (no duplicate tombstone).
        self.assertEqual(read_key(19, 'k2'), (0, 'v2_orig'),   'k2 at T=19: original stable value')
        self.assertEqual(read_key(25, 'k2'), (0, 'v2_ingest'), 'k2 at T=25: ingest write visible')
        self.assertEqual(read_key(30, 'k2')[0], wiredtiger.WT_NOTFOUND, 'k2 absent at truncate ts')

        rsession.close()


if __name__ == '__main__':
    wttest.run()
