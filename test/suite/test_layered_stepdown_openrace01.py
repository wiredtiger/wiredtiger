#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or personal use, and by any means.

import sys, threading, time, wttest
from helper_disagg import disagg_test_class

# test_layered_stepdown_openrace01.py
#   Reproduction for the "open btree / open dhandle vs primary step down" race.
#
# RCA (open-path race): the step-down sweep
# (__disagg_mark_btrees_readonly_then_step_down) marks every *open* disaggregated
# btree WT_BTREE_READONLY, then sets leader = false. It skips any dhandle that is
# still being opened (WT_DHANDLE_OPEN not yet set). A btree open that started on
# the leader path but has not yet published its open flag therefore slips past
# the sweep and is never marked read-only, leaving a disaggregated handle
# writable on a follower.
#
#   Thread A (open)                     Thread B (reconfigure step-down)
#   --------------                      --------------------------------
#   __wt_conn_dhandle_open()
#     __wt_btree_open() done
#     [timing stress: sleep]
#                                       __disagg_step_down() sweep:
#                                         handle skipped (open flag unset)
#                                         leader = false
#     F_SET(WT_DHANDLE_OPEN)
#   -> handle open read-write on a follower
#
# Dirtying that escaped handle and evicting it drives reconciliation to the
# disaggregated block manager write, where the leader-only guard fires. In a
# diagnostic build the earlier dirty-time assertion (__wt_page_modify_set) fires
# first; in a release build it is block_disagg_write.c:121 ("Trying to write the
# page from a follower"). Both have the same predicate (disagg && !leader).
#
# The timing-stress knob disagg_open_btree_slow widens Thread A's window so the
# step-down deterministically lands inside it.
@disagg_test_class
class test_layered_stepdown_openrace01(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(lose_all_my_data=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=i,value_format=S'

    uri = 'layered:test_layered_stepdown_openrace01'
    stable_file_uri = 'file:test_layered_stepdown_openrace01.wt_stable'

    nrows = 200
    value = 'x' * 1024

    # When True, inject the open-btree timing stress to open the race
    # window (expected: abort). When False (negative control), the open completes
    # before step-down, the handle is marked read-only normally, and the insert
    # silently no-ops with no abort.
    stress = True

    def test_open_race(self):
        if sys.platform.startswith('darwin'):
            return

        # Leader: create + populate + checkpoint so the stable component is
        # materialized to shared storage.
        self.session.create(self.uri, self.create_session_config)
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nrows):
            cursor[i] = self.value
        cursor.close()
        self.session.checkpoint()

        # Second node. The open-btree stress widens the race window. We must
        # observe a *fresh* __wt_conn_dhandle_open of the stable btree during the
        # race, so the stable handle must not stay cached open beforehand:
        #   - no statistics_log: the statlog server otherwise re-opens the stable
        #     handle every second for stats, pinning it open;
        #   - aggressive file_manager sweep so the idle stable handle the step-up
        #     ingest drain opens is closed again before Thread A runs.
        follow_config = self.extensionsConfig() + ',create,' \
            + 'statistics=(all),disaggregated=(lose_all_my_data=true),' \
            + 'file_manager=(close_handle_minimum=0,close_idle_time=1,' \
            + 'close_scan_interval=1),' \
            + ('timing_stress_for_test=[disagg_open_btree_slow],'
               if self.stress else '') \
            + 'disaggregated=(role="follower")'
        conn_follow = self.wiredtiger_open('follower', follow_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, self.create_session_config)
        self.disagg_advance_checkpoint(conn_follow, self.conn)

        # Step the second node up to leader so Thread A enters the open via the
        # leader path (leader == true when the open starts).
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Let the sweep server close the stable handle that the step-up ingest
        # drain opened, so Thread A's open below is a genuine fresh open that
        # hits the timing-stress point.
        time.sleep(4)

        # Thread A: open the live stable handle. The stress delays the open just
        # before the open flag is published. Record how long the open took: a
        # ~2s open means a fresh __wt_conn_dhandle_open hit the stress point
        # (the race window opened); a near-instant open means the handle was
        # already cached open and no fresh open (hence no race) occurred.
        timings = {}
        def open_stable():
            s = conn_follow.open_session('')
            start = time.time()
            try:
                c = s.open_cursor(self.stable_file_uri, None, 'overwrite')
                c.close()
            except Exception as e:
                timings['err'] = repr(e)
            timings['open_secs'] = time.time() - start
            s.close()

        t = threading.Thread(target=open_stable)
        t.start()

        # Let Thread A reach the delayed open, then step the node down. The sweep
        # skips Thread A's not-yet-open handle, so it is never marked read-only;
        # leader becomes false.
        time.sleep(0.5)
        conn_follow.reconfigure('disaggregated=(role="follower")')
        t.join()

        # The stable handle is now open read-write on a follower. Dirtying it is
        # where __wt_page_modify_set's diagnostic leader-only assertion
        # (btree_inline.h, predicate disagg && !leader) fires, because the handle
        # escaped the step-down read-only sweep. This is the open/step-down race; in a
        # release build the same predicate fires one layer deeper at
        # block_disagg_write.c:121 during reconciliation.
        cursor = session_follow.open_cursor(self.stable_file_uri, None, 'overwrite')
        session_follow.begin_transaction()
        for i in range(self.nrows):
            cursor.set_key(i)
            cursor.set_value(self.value)
            cursor.insert()
        session_follow.commit_transaction()
        cursor.close()

        # In a release build the dirty-time assertion above is compiled out, so
        # the insert succeeds. Force synchronous eviction of the dirty stable
        # pages: reconciliation reaches the disaggregated block manager write,
        # where the leader-only guard (block_disagg_write.c, "Trying to write the
        # page from a follower") fires because this node is a follower.
        evict_session = conn_follow.open_session('debug=(release_evict_page=true)')
        evict_session.begin_transaction('isolation=snapshot')
        evict_cursor = evict_session.open_cursor(self.stable_file_uri, None, None)
        for i in range(self.nrows):
            evict_cursor.set_key(i)
            if evict_cursor.search() == 0:
                evict_cursor.reset()
        evict_cursor.close()
        evict_session.rollback_transaction()
