#!/usr/bin/env python3
#
# stepdown_tmp.py
#    Scratch file for WT-18220: the open gaps from the async step-down coverage matrix, written as
#    runnable tests so we can see which ones the current code already satisfies.
#
#    Not for review or commit. Gap numbering follows the coverage doc.

import signal
import wiredtiger, wttest
from wiredtiger import stat
from suite_subprocess import suite_subprocess
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class stepdown_tmp(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    def conn_stat(self, which):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[which][2]
        stat_cursor.close()
        return value

    # Open a cursor and assert the layered handle came from the session cursor cache.
    def open_cached_cursor(self, uri, config=None):
        before = self.conn_stat(stat.conn.cursor_reopen)
        cursor = self.session.open_cursor(uri, None, config)
        self.assertEqual(self.conn_stat(stat.conn.cursor_reopen) - before, 1,
            'the layered cursor must be served from the session cursor cache')
        return cursor

    # Force every key of a constituent out of cache so older versions must come from the history
    # store on a later read.
    def evict_constituent(self, uri, keys):
        evict_cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        self.session.begin_transaction()
        for key in keys:
            evict_cursor.set_key(key)
            if evict_cursor.search() == 0:
                evict_cursor.reset()
        self.session.rollback_transaction()
        evict_cursor.close()

    # gap 1: a committed stable update on the leader must conflict with a follower write that
    # carries a read timestamp below it, even though the two updates live in different tables.
    def test_gap1_conflict_stable_leader_vs_follower_older_read_ts(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'leader'}, 10)

        self.set_step_down_ts(20)
        self.complete_step_down(20)

        # The follower writer reads as of before the stable commit, so it cannot see the update it
        # would be overwriting.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(5))
        cursor.set_key('k1')
        cursor.set_value('follower')
        self.expect_conflict_rollback(cursor.update)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'leader'})

    # gap 2: the visibility split just below, at and just above the cutoff must be exact once the
    # step-down has completed.
    def test_gap2_boundary_reads_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Below and at the cutoff must be committed before it is set: a commit at or below the
        # cutoff is rejected once the timestamp is armed.
        self.write_at(self.uri, {'below': 'v'}, 18)
        self.write_at(self.uri, {'at': 'v'}, 20)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'above': 'v'}, 22)

        self.complete_step_down(20)

        self.assertEqual(self.read_keys_at(self.uri, 17), set())
        self.assertEqual(self.read_keys_at(self.uri, 18), {'below'})
        self.assertEqual(self.read_keys_at(self.uri, 19), {'below'})
        self.assertEqual(self.read_keys_at(self.uri, 20), {'below', 'at'})
        self.assertEqual(self.read_keys_at(self.uri, 21), {'below', 'at'})
        self.assertEqual(self.read_keys_at(self.uri, 22), {'below', 'at', 'above'})

    # gap 3: largest_key ignores visibility by contract, so a removed maximum still counts.
    def test_gap3_largest_key_after_removing_largest(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'a': 'v', 'm': 'v', 'z': 'v'}, 10)

        self.set_step_down_ts(20)
        self.remove_at(self.uri, ['z'], 30)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), 'z',
            'a removed maximum still counts for largest_key')
        cursor.close()

        self.complete_step_down(20)
        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), 'z',
            'the removed maximum still counts after the demotion')
        cursor.close()

    # gap 4: a table dropped while the timestamp is set must not resurface on the follower.
    def test_gap4_drop_stays_dropped_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        self.set_step_down_ts(20)
        self.dropUntilSuccess(self.session, self.uri)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.uri, None, None))

        self.complete_step_down(20)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.uri, None, None))

    # gap 5: a write through a cursor served from the session cursor cache after the demotion must
    # route to ingest.
    def test_gap5_write_through_cache_served_cursor_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'pre': 'stable'}, 10)

        self.set_step_down_ts(20)
        self.complete_step_down(20)

        cursor = self.open_cached_cursor(self.uri)
        self.session.begin_transaction()
        cursor['post'] = 'ingest'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'post'})
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 40), {'pre'})
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'pre': 'stable', 'post': 'ingest'})

    # gap 7: sampling after the demotion with content in both constituents.
    def test_gap7_next_random_after_step_down_mixed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        stable_keys = {f's{i:02d}' for i in range(10)}
        self.write_at(self.uri, {k: 's' for k in stable_keys}, 10)

        self.set_step_down_ts(20)
        ingest_keys = {f'i{i:02d}' for i in range(10)}
        self.write_at(self.uri, {k: 'i' for k in ingest_keys}, 30)

        removed = {'s00', 'i00'}
        self.remove_at(self.uri, sorted(removed), 40)
        visible = (stable_keys | ingest_keys) - removed

        self.complete_step_down(20)
        self.assertEqual(self.read_keys_at(self.uri, 50), visible,
            'the merged view must be unchanged by the demotion')

        cursor = self.session.open_cursor(self.uri, None, 'next_random=true')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        for _ in range(100):
            self.assertEqual(cursor.next(), 0)
            self.assertIn(cursor.get_key(), visible,
                'a sample must come from the visible merged view')
        self.session.rollback_transaction()
        cursor.close()

    # cross gap 1 (C2xC5): the straddler guard must fire on a cursor served from the session cursor
    # cache, whose reopen path differs from a cold open.
    def test_crossgap1_straddler_through_cache_served_cursor(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)

        # Prime the cursor cache, then start the straddling transaction.
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor.close()
        self.session.begin_transaction()

        self.set_step_down_ts(20)

        cursor = self.open_cached_cursor(self.uri)
        def straddle_write():
            cursor['k2'] = 'v'
        self.assert_step_down_rollback(straddle_write)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'base'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())

    # cross gap 1, second shape: the handle was last used by an earlier committed transaction.
    def test_crossgap1_straddler_through_handle_used_by_earlier_txn(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'base'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        self.session.begin_transaction()

        self.set_step_down_ts(20)

        cursor = self.open_cached_cursor(self.uri)
        def straddle_write():
            cursor['k2'] = 'v'
        self.assert_step_down_rollback(straddle_write)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'base'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())

    # cross gap 2 (C1xC4): duplicate-key and overwrite=false semantics when the conflicting key
    # lives in ingest, which is consulted first, rather than in stable.
    def test_crossgap2_overwrite_false_against_ingest_resident_keys(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # 'shadowed' is seeded in stable so its ingest record can be a tombstone.
        self.write_at(self.uri, {'shadowed': 'stable'}, 10)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'ingest_only': 'ingest'}, 30)
        self.remove_at(self.uri, ['shadowed'], 31)

        cursor = self.session.open_cursor(self.uri, None, 'overwrite=false')

        # A duplicate that lives in ingest must be detected.
        self.session.begin_transaction()
        cursor.set_key('ingest_only')
        cursor.set_value('again')
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.insert(),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_DUPLICATE_KEY))
        self.session.rollback_transaction()

        # An ingest-resident key can be updated and removed.
        self.session.begin_transaction()
        cursor.set_key('ingest_only')
        cursor.set_value('updated')
        self.assertEqual(cursor.update(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(32))

        # A key whose ingest record is a tombstone over stable is gone for both ops.
        self.session.begin_transaction()
        cursor.set_key('shadowed')
        cursor.set_value('v')
        self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        cursor.set_key('shadowed')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # An insert over that tombstone is not a duplicate.
        self.session.begin_transaction()
        cursor.set_key('shadowed')
        cursor.set_value('reborn')
        self.assertEqual(cursor.insert(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(33))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40),
            {'ingest_only': 'updated', 'shadowed': 'reborn'})

    # cross gap 3 (C4xC5): bounds whose edges need cross-constituent resolution -- an ingest-only
    # key, an ingest tombstone over stable, and a gap between constituents.
    def test_crossgap3_bounds_on_ingest_resident_edges(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's', 'h': 's'}, 10)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'c': 'i', 'e': 'i'}, 30)
        self.remove_at(self.uri, ['f'], 31)

        def bounded_walk(lower, upper, reverse=False):
            cursor = self.session.open_cursor(self.uri, None, None)
            cursor.set_key(lower)
            self.assertEqual(cursor.bound('action=set,bound=lower'), 0)
            cursor.set_key(upper)
            self.assertEqual(cursor.bound('action=set,bound=upper'), 0)
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
            seen = []
            while (cursor.prev() if reverse else cursor.next()) == 0:
                seen.append(cursor.get_key())
            self.session.rollback_transaction()
            cursor.close()
            return seen

        # Lower bound is an ingest-only key, upper bound is an ingest tombstone over stable.
        self.assertEqual(bounded_walk('c', 'f'), ['c', 'd', 'e'])
        self.assertEqual(bounded_walk('c', 'f', reverse=True), ['e', 'd', 'c'])

        # Both edges fall in gaps: 'a' is below everything, 'g' between the tombstone and 'h'.
        self.assertEqual(bounded_walk('a', 'g'), ['b', 'c', 'd', 'e'])
        self.assertEqual(bounded_walk('a', 'g', reverse=True), ['e', 'd', 'c', 'b'])

        # The same bounds still clamp after the demotion.
        self.complete_step_down(20)
        self.assertEqual(bounded_walk('c', 'f'), ['c', 'd', 'e'])
        self.assertEqual(bounded_walk('c', 'f', reverse=True), ['e', 'd', 'c'])

    # missing class 1: the reader-survival design rests on the history store, but no test makes a
    # read actually resolve through it. Build several versions per key, evict so the old ones live
    # in the history store, then read at old timestamps in each phase.
    def test_missing_class1_reads_through_history_store(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = [f'k{i:02d}' for i in range(10)]
        for ts, tag in ((10, 'v10'), (14, 'v14'), (18, 'v18')):
            self.write_at(self.uri, {k: tag for k in keys}, ts)

        # Checkpoint then force the stable constituent out of cache: reconciliation moves the
        # superseded versions to the history store.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.evict_constituent(self.stable_uri(self.uri), keys)

        hs_before = self.conn_stat(stat.conn.cache_hs_read)
        self.assertEqual(self.read_kvs_at(self.uri, 10), {k: 'v10' for k in keys})
        self.assertEqual(self.read_kvs_at(self.uri, 14), {k: 'v14' for k in keys})
        self.assertGreater(self.conn_stat(stat.conn.cache_hs_read), hs_before,
            'the as-of-past reads must resolve through the history store')

        # The same reads while the timestamp is set, with newer versions in ingest.
        self.set_step_down_ts(20)
        self.write_at(self.uri, {k: 'v30' for k in keys}, 30)

        hs_before = self.conn_stat(stat.conn.cache_hs_read)
        self.assertEqual(self.read_kvs_at(self.uri, 14), {k: 'v14' for k in keys})
        self.assertEqual(self.read_kvs_at(self.uri, 18), {k: 'v18' for k in keys})
        self.assertEqual(self.read_kvs_at(self.uri, 30), {k: 'v30' for k in keys})
        self.assertGreater(self.conn_stat(stat.conn.cache_hs_read), hs_before,
            'the history store must still answer as-of-past reads while the timestamp is set')

        # And after the demotion, where stable is served from the step-down checkpoint.
        self.complete_step_down(20)
        hs_before = self.conn_stat(stat.conn.cache_hs_read)
        self.assertEqual(self.read_kvs_at(self.uri, 14), {k: 'v14' for k in keys})
        self.assertEqual(self.read_kvs_at(self.uri, 18), {k: 'v18' for k in keys})
        self.assertEqual(self.read_kvs_at(self.uri, 30), {k: 'v30' for k in keys})
        self.assertGreater(self.conn_stat(stat.conn.cache_hs_read), hs_before,
            'the history store must answer as-of-past reads after the demotion')

    # missing class 1, second half: the oldest timestamp advances mid-transition, which is the one
    # injectable event no test crosses.
    def test_missing_class1_oldest_advances_mid_transition(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v10'}, 10)
        self.write_at(self.uri, {'k1': 'v14'}, 14)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'k1': 'v30'}, 30)

        # A held reader pins its read timestamp; oldest may not advance past it, but it may advance
        # past the older version.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(14))
        self.assertEqual(cursor['k1'], 'v14')

        # Oldest may not pass stable, and stable may not pass the cutoff, so advance both to 14.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(14) +
                                ',oldest_timestamp=' + self.timestamp_str(14))

        self.assertEqual(cursor['k1'], 'v14',
            'the held reader must keep seeing its own version after oldest advances')

        self.complete_step_down(20)
        self.assertEqual(cursor['k1'], 'v14',
            'and after the demotion')
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 30), {'k1': 'v30'})

    # gap 1, variant: no read timestamp, so the stable update is visible and the write is legal.
    def test_gap1_variant_follower_write_no_read_ts_succeeds(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'leader'}, 10)

        self.set_step_down_ts(20)
        self.complete_step_down(20)

        self.write_at(self.uri, {'k1': 'follower'}, 30)
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'follower'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1'})

    # gap 1, variant: the earlier update was committed while the timestamp was set, so it lives in
    # ingest; the follower writer with the older read timestamp must still conflict.
    def test_gap1_variant_conflict_ingest_update_vs_older_read_ts(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'k1': 'armed'}, 30)
        self.complete_step_down(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        cursor.set_key('k1')
        cursor.set_value('follower')
        self.expect_conflict_rollback(cursor.update)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'armed'})

    # Negative control for this file: WT-18156 is a known limitation -- content served from the
    # step-down checkpoint through freshly opened handles loses snapshot-based invisibility. A
    # reader with no read timestamp whose snapshot predates a stable commit must not see it. This is
    # expected to FAIL until WT-18156 is fixed; it proves these tests can detect a real defect.
    def test_control_wt18156_snapshot_invisibility_lost(self):
        self.skipTest('FIXME-WT-18156: reproduces the known invisibility loss, fixed elsewhere')
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'seen': 'v'}, 10)

        # The reader's snapshot is taken before the second stable commit exists.
        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        self.assertEqual(rcur['seen'], 'v')

        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur['unseen'] = 'v'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(12))
        wcur.close()
        wsession.close()

        rcur.set_key('unseen')
        self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND,
            'the post-snapshot commit must be invisible before the step-down')

        self.set_step_down_ts(20)
        self.complete_step_down(20)

        rcur.reset()
        rcur.set_key('unseen')
        self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND,
            'the post-snapshot commit must still be invisible after the demotion')
        self.session.rollback_transaction()
        rcur.close()

    # Which event makes the invisible commit visible: measured E4 (the reconfigure), not the
    # step-down checkpoint. P0/P1/P2/P3 invisible, P4 VISIBLE.

    # missing class 1, completion: multiple versions on BOTH sides, both constituents evicted, and
    # as-of-past reads in every phase including P2 and P3.
    def test_missing_class1_history_store_both_constituents(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = [f'k{i:02d}' for i in range(10)]
        for ts, tag in ((10, 'v10'), (14, 'v14'), (18, 'v18')):
            self.write_at(self.uri, {k: tag for k in keys}, ts)

        self.set_step_down_ts(20)

        # Several ingest versions, so the ingest constituent has its own chains to age out.
        for ts, tag in ((30, 'v30'), (34, 'v34'), (38, 'v38')):
            self.write_at(self.uri, {k: tag for k in keys}, ts)

        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.evict_constituent(self.stable_uri(self.uri), keys)
        self.evict_constituent(self.ingest_uri(self.uri), keys)

        def check_all_phases(label):
            hs_before = self.conn_stat(stat.conn.cache_hs_read)
            for ts, tag in ((10, 'v10'), (14, 'v14'), (18, 'v18'),
                            (30, 'v30'), (34, 'v34'), (38, 'v38')):
                self.assertEqual(self.read_kvs_at(self.uri, ts), {k: tag for k in keys},
                    f'as-of-past read at {ts} wrong in {label}')
            self.assertGreater(self.conn_stat(stat.conn.cache_hs_read), hs_before,
                f'reads in {label} must resolve through the history store')

        check_all_phases('P1')

        # P2: stable advances to the cutoff.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        check_all_phases('P2')

        # P3: the step-down checkpoint has been taken, still a leader.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        check_all_phases('P3')

        # P4: follower.
        self.conn.reconfigure('disaggregated=(role="follower")')
        check_all_phases('P4')

    # missing class 1: oldest advances mid-transition with the old versions actually in the history
    # store, and a held reader pinned below the new oldest boundary.
    def test_missing_class1_oldest_advances_with_history_store(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = [f'k{i:02d}' for i in range(10)]
        for ts, tag in ((10, 'v10'), (14, 'v14'), (18, 'v18')):
            self.write_at(self.uri, {k: tag for k in keys}, ts)

        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.evict_constituent(self.stable_uri(self.uri), keys)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {k: 'v30' for k in keys}, 30)

        # A reader pinned at 14 holds its snapshot while oldest advances to exactly its read
        # timestamp, then across the rest of the step-down.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(14))
        self.assertEqual(cursor['k00'], 'v14')

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(14) +
                                ',oldest_timestamp=' + self.timestamp_str(14))
        self.assertEqual(cursor['k00'], 'v14', 'pinned read survives oldest advancing to it')

        self.complete_step_down(20)
        for k in keys:
            self.assertEqual(cursor[k], 'v14', 'pinned read survives the demotion')
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 30), {k: 'v30' for k in keys})

    # missing class 3: verify() while the timestamp is set. The feature relaxed verify so a leader's
    # ingest constituent may legitimately hold content. A freshly checkpointed layered table reports
    # EBUSY on its first checkpoint regardless of the cutoff, so both runs checkpoint twice.
    def test_missing_class3_verify_while_step_down_ts_set(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'stable_key': 'v'}, 10)

        def verify_all(label):
            for _ in range(2):
                ckpt_session = self.conn.open_session()
                ckpt_session.checkpoint()
                ckpt_session.close()
            for name, uri in (('layered', self.uri), ('ingest', self.ingest_uri(self.uri)),
                              ('stable', self.stable_uri(self.uri))):
                vsession = self.conn.open_session()
                vsession.verify(uri, None)
                vsession.close()

        verify_all('no_cutoff')

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'ingest_key': 'v'}, 30)
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'ingest_key'})

        # The relaxation: content in the ingest constituent must not make verify object.
        verify_all('cutoff_set_with_ingest_content')

    # missing class 3: the other maintenance operations have no pinned behavior while the timestamp
    # is set. Record what each one does rather than asserting a contract we have not agreed.
    def test_missing_class3_maintenance_ops_while_step_down_ts_set(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        self.set_step_down_ts(20)
        self.write_at(self.uri, {'k2': 'v'}, 30)

        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

        outcomes = {}
        def record(label, fn):
            try:
                fn()
                outcomes[label] = 'ok'
            except wiredtiger.WiredTigerError as e:
                outcomes[label] = 'error: %s' % str(e)[:60]

        record('compact', lambda: self.session.compact(self.uri, None))
        record('rollback_to_stable', lambda: self.conn.rollback_to_stable())
        record('alter', lambda: self.session.alter(self.uri, 'access_pattern_hint=random'))
        record('salvage', lambda: self.session.salvage(self.stable_uri(self.uri), None))
        record('backup_cursor',
            lambda: self.session.open_cursor('backup:', None, None).close())

        self.ignoreStderrPatternIfExists('Compaction does not work with disaggregated storage')
        self.ignoreStderrPatternIfExists('unsupported object operation')
        self.pr('MAINTENANCE ' + str(outcomes))
        # Nothing is asserted: this test exists to report the current behavior for the doc.

    # missing class 4: role-change edges. Each transition must produce a defined result.
    def test_missing_class4_role_change_edges(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        self.set_step_down_ts(20)

        outcomes = {}
        def record(label, fn):
            try:
                fn()
                outcomes[label] = 'ok'
            except wiredtiger.WiredTigerError as e:
                outcomes[label] = 'error: %s' % str(e)[:60]

        # Reconfigure back to leader while the timestamp is set: there is no way to unset it.
        record('reconfigure_leader_while_ts_set',
            lambda: self.conn.reconfigure('disaggregated=(role="leader")'))
        self.pr('ROLE_EDGES after leader attempt, ts_set=%d' % self.step_down_ts_is_set())

        # Complete the step-down, then repeat the demotion.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        record('reconfigure_follower_first',
            lambda: self.conn.reconfigure('disaggregated=(role="follower")'))
        record('reconfigure_follower_repeated',
            lambda: self.conn.reconfigure('disaggregated=(role="follower")'))

        # A step-up with ingest content present, outside the normal drain path.
        self.write_at(self.uri, {'k2': 'v'}, 30)
        record('step_up_with_ingest_content',
            lambda: self.conn.reconfigure('disaggregated=(role="leader")'))

        self.pr('ROLE_EDGES ' + str(outcomes))
        self.assertEqual(self.read_keys_at(self.uri, 40), {'k1', 'k2'},
            'content must survive whatever the role edges do')

# missing class 5: eviction while the timestamp is set. Its own class so the cache can be small.
# A 2MB cache with no checkpoint stalls the committing thread in __wti_evict_app_assist_worker for a
# plain leader too, so this drives eviction the way production does: periodic checkpoints.
@disagg_test_class
class stepdown_tmp_eviction(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = ('statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
                        'cache_size=2MB,eviction_target=20,eviction_trigger=40,')
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    def conn_stat(self, which):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[which][2]
        stat_cursor.close()
        return value

    def test_missing_class5_eviction_while_step_down_ts_set(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        value = 'v' * 2000
        stable_keys = {f's{i:04d}' for i in range(50)}
        self.write_at(self.uri, {k: value for k in stable_keys}, 10)

        self.set_step_down_ts(20)

        before = self.conn_stat(stat.conn.cache_eviction_pages_seen)

        ingest_keys = {f'i{i:04d}' for i in range(300)}
        cursor = self.session.open_cursor(self.uri, None, None)
        for i, k in enumerate(sorted(ingest_keys)):
            self.session.begin_transaction()
            cursor[k] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30 + i))
            if (i + 1) % 50 == 0:
                ckpt_session = self.conn.open_session()
                ckpt_session.checkpoint()
                ckpt_session.close()
        cursor.close()

        self.assertGreater(self.conn_stat(stat.conn.cache_eviction_pages_seen), before,
            'the workload must have driven eviction during the transition')

        self.assertEqual(self.read_keys_at(self.uri, 10000), stable_keys | ingest_keys,
            'the merged view must survive eviction during the transition')

        self.complete_step_down(20)
        self.assertEqual(self.read_keys_at(self.uri, 10000), stable_keys | ingest_keys,
            'and after the demotion')

# Class 7, remaining rows: operations whose guard is an assertion that kills the process. Truncate
# and prepare are excluded (owned elsewhere); these two are the WT_ASSERT rows, so they need a
# diagnostic build. Each scenario runs in a subprocess so the abort is observed as a signal.
_abort_ops = [
    ('autocommit_layered_write', dict(op='autocommit')),
    ('read_committed', dict(op='read_committed')),
    ('read_uncommitted', dict(op='read_uncommitted')),
]

@disagg_test_class
class stepdown_tmp_aborts(LayeredStepdownMixin, wttest.WiredTigerTestCase, suite_subprocess):
    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, _abort_ops)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    # The body that is expected to abort. Runs in the subprocess.
    def subprocess_forbidden_op(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)

        self.set_step_down_ts(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        if self.op == 'autocommit':
            # No explicit transaction: the layered operation begins one internally, after the
            # routing decision has been made, so it evades the guard and must abort.
            cursor['k2'] = 'v'
        else:
            isolation = 'read-committed' if self.op == 'read_committed' else 'read-uncommitted'
            self.session.begin_transaction('isolation=' + isolation)
            cursor.set_key('k1')
            cursor.search()
            self.session.rollback_transaction()
        cursor.close()

    def test_forbidden_op_aborts(self):
        rc, _ = self.run_subprocess_function(
            'SUBPROCESS',
            'stepdown_tmp.stepdown_tmp_aborts.subprocess_forbidden_op',
            silent=True,
            scenario=self.scenario_name)
        self.assertEqual(rc, -signal.SIGABRT,
            f'expected the guard to abort (rc={-signal.SIGABRT}) but got rc={rc}')

# missing class 2 (Class 5b): cursor/transaction binding. A handle whose child cursors were built
# for one phase is reused in another; the resumed operation must read the right trees and route its
# write to the right constituent.
def _op_scan_from_reset(test, cursor):
    cursor.reset()
    seen = set()
    while cursor.next() == 0:
        seen.add(cursor.get_key())
    return ('read', seen)

def _op_search(test, cursor):
    cursor.set_key('k1')
    test.assertEqual(cursor.search(), 0)
    return ('read', {cursor.get_key()})

def _op_search_near(test, cursor):
    cursor.set_key('k0')
    test.assertNotEqual(cursor.search_near(), wiredtiger.WT_NOTFOUND)
    return ('read', {cursor.get_key()})

def _op_largest_key(test, cursor):
    test.assertEqual(cursor.largest_key(), 0)
    return ('read', {cursor.get_key()})

def _op_insert(test, cursor):
    cursor['new_key'] = 'v'
    return ('write', 'new_key')

def _op_update(test, cursor):
    cursor.set_key('k1')
    cursor.set_value('updated')
    test.assertEqual(cursor.update(), 0)
    return ('write', 'k1')

def _op_remove(test, cursor):
    cursor.set_key('k1')
    test.assertEqual(cursor.remove(), 0)
    return ('write', 'k1')

def _op_reserve(test, cursor):
    cursor.set_key('k1')
    test.assertEqual(cursor.reserve(), 0)
    return ('reserve', 'k1')

_resumed_ops = [
    ('scan_from_reset', dict(resume=_op_scan_from_reset)),
    ('search', dict(resume=_op_search)),
    ('search_near', dict(resume=_op_search_near)),
    ('largest_key', dict(resume=_op_largest_key)),
    ('insert', dict(resume=_op_insert)),
    ('update', dict(resume=_op_update)),
    ('remove', dict(resume=_op_remove)),
    ('reserve', dict(resume=_op_reserve)),
]

# How the handle is reused for the second operation.
_reuse_patterns = [
    ('same_handle', dict(reuse='same_handle')),
    ('cache_served', dict(reuse='cache_served')),
    ('close_reopen_in_txn', dict(reuse='close_reopen_in_txn')),
]

# What happened to the handle between the two uses.
_windows = [
    ('set', dict(window='set')),
    ('full', dict(window='full')),
]

# How the first transaction ended. 'guard' means the step-down guard fired mid-operation, which
# leaves a half-entered handle no clean ending produces.
_prev_endings = [
    ('commit', dict(prev_end='commit')),
    ('rollback', dict(prev_end='rollback')),
    ('guard', dict(prev_end='guard')),
]

@disagg_test_class
class stepdown_tmp_binding(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, _reuse_patterns, _resumed_ops, _windows,
                               _prev_endings)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    def conn_stat(self, which):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[which][2]
        stat_cursor.close()
        return value

    def test_binding_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base', 'k2': 'base'}, 10)

        cursor = self.session.open_cursor(self.uri, None, None)

        # First use of the handle, before the cutoff is set.
        if self.prev_end == 'guard':
            # The guard must fire on this transaction's write once the cutoff is set, which is the
            # ending no clean commit or rollback produces.
            self.session.begin_transaction()
            cursor.set_key('k1')
            self.assertEqual(cursor.search(), 0)
            self.set_step_down_ts(20)
            self.assert_step_down_rollback(lambda: cursor.__setitem__('straddle', 'v'))
            self.session.rollback_transaction()
        else:
            self.session.begin_transaction()
            cursor.set_key('k1')
            self.assertEqual(cursor.search(), 0)
            if self.prev_end == 'commit':
                self.session.commit_transaction()
            else:
                self.session.rollback_transaction()
            self.set_step_down_ts(20)

        # Ingest content exists in every window, so the merged view is genuinely two-sided.
        self.write_at(self.uri, {'ingest_key': 'i'}, 30)

        if self.window == 'full':
            self.complete_step_down(20)

        # Second use of the handle, in the new phase.
        if self.reuse == 'cache_served':
            cursor.close()
            before = self.conn_stat(stat.conn.cursor_reopen)
            cursor = self.session.open_cursor(self.uri, None, None)
            self.assertEqual(self.conn_stat(stat.conn.cursor_reopen) - before, 1,
                'the handle must have come from the session cursor cache')

        self.session.begin_transaction()

        if self.reuse == 'close_reopen_in_txn':
            cursor.close()
            cursor = self.session.open_cursor(self.uri, None, None)

        kind, result = self.resume(self, cursor)

        if kind == 'read':
            merged = {'k1', 'k2', 'ingest_key'}
            if self.resume is _op_scan_from_reset:
                self.assertEqual(result, merged,
                    'the resumed scan must see the whole merged view')
            elif self.resume is _op_largest_key:
                self.assertEqual(result, {'k2'},
                    'largest_key must report the largest key of the merged view')
            else:
                self.assertTrue(result.issubset(merged),
                    f'the resumed read returned a key outside the merged view: {result}')
            self.session.rollback_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(40))
            # Every write in either window routes to ingest: the cutoff is set in 'set' and the node
            # is a follower in 'full'.
            if kind == 'write':
                self.assertIn(result, self.read_keys_at(self.ingest_uri(self.uri), 50),
                    'the resumed write must land in the ingest constituent')
                # Values, not just keys: an update or remove of a stable-resident key is only
                # provably routed to ingest if stable still holds the original value.
                self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 50),
                    {'k1': 'base', 'k2': 'base'},
                    'the resumed write must not touch the stable constituent')
                merged = self.read_kvs_at(self.uri, 50)
                if self.resume is _op_insert:
                    self.assertEqual(merged.get('new_key'), 'v')
                elif self.resume is _op_update:
                    self.assertEqual(merged.get('k1'), 'updated')
                    self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 50).get('k1'),
                        'updated', 'the new value must be the ingest record')
                elif self.resume is _op_remove:
                    self.assertNotIn('k1', merged)
                    self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 50).get('k1'),
                        'base', 'the removal must be a tombstone in ingest, not a stable delete')

        cursor.close()
