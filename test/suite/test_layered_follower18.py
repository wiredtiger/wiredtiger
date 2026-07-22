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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wiredtiger import stat
from helper import WiredTigerCursor, statistic_uri

# test_layered_follower18.py
#    Follower selective checkpoint pickup: with checkpoint_pickup_defer_period set, a follower
#    advances only a subset of layered tables' stable checkpoints at each pickup, deferring the rest,
#    while still returning correct data. Deferral is disabled by default, is reconfigurable, and a
#    step-up forces a full pickup.
#
#    Only a table with an open ingest dhandle (the ones a reader would hit) and an existing local
#    stable entry (i.e. picked up at least once before) is a deferral candidate, so the follower is
#    kept caught up and warmed up before the pickups that exercise deferral.
@disagg_test_class
class test_layered_follower18(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'

    scenarios = gen_disagg_storages(disagg_only = True)

    ntables = 20
    # A period coprime to the ingest btree id spacing spreads the forced round-robin pickups across
    # the tables, so a single pickup advances some tables and defers others.
    period = 7

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def follower_config(self, defer_period):
        # A high per-table min_kb keeps the byte threshold from forcing pickups, so deferral is
        # driven purely by the round-robin period and is deterministic for the test.
        return self.extensionsConfig() + self.conn_base_config + \
            'disaggregated=(role="follower",' + \
            f'checkpoint_pickup_defer_period={defer_period},' + \
            'checkpoint_pickup_defer_min_kb=1048576)'

    def uri(self, i):
        return f'layered:{self.test_name}_{i}'

    def stat_value(self, session, which):
        with WiredTigerCursor(session, statistic_uri()) as c:
            return c[which][2]

    def make_tables(self, session):
        for i in range(self.ntables):
            session.create(self.uri(i), 'key_format=S,value_format=S')

    # Populate every table on the leader with a fresh batch, then checkpoint.
    def leader_write_checkpoint(self, oplog, thandles, base):
        for t in thandles:
            oplog.insert(t, 50)
        oplog.apply(self, self.session, base, 50 * self.ntables)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()
        return base + 50 * self.ntables

    # Leader writes and checkpoints, the follower catches its ingest up to the same point (which
    # opens every ingest dhandle so it is a deferral candidate), then the follower picks up.
    def cycle(self, oplog, thandles, session_follow, conn_follow, pos):
        pos = self.leader_write_checkpoint(oplog, thandles, pos)
        oplog.apply(self, session_follow, 0, pos)
        self.disagg_advance_checkpoint(conn_follow)
        return pos

    def setup_follower(self, defer_period):
        oplog = Oplog()
        self.make_tables(self.session)
        thandles = [oplog.add_uri(self.uri(i)) for i in range(self.ntables)]

        conn_follow = self.wiredtiger_open('follower', self.follower_config(defer_period))
        session_follow = conn_follow.open_session('')
        self.make_tables(session_follow)
        return oplog, thandles, conn_follow, session_follow

    def deferred(self, session_follow):
        return self.stat_value(session_follow, stat.conn.disagg_pick_up_file_meta_deferred)

    def updated(self, session_follow):
        return self.stat_value(session_follow, stat.conn.disagg_pick_up_file_meta_updated)

    def test_selective_pickup(self):
        oplog, thandles, conn_follow, session_follow = self.setup_follower(self.period)

        # The first pickup creates the local stable entries (inserts); deferral applies only to the
        # updates on later pickups.
        pos = self.cycle(oplog, thandles, session_follow, conn_follow, 0)
        self.assertEqual(self.deferred(session_follow), 0)

        # A single later pickup advances some tables and defers others.
        d0, u0 = self.deferred(session_follow), self.updated(session_follow)
        pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        advanced = self.updated(session_follow) - u0
        self.assertGreater(self.deferred(session_follow) - d0, 0)
        self.assertGreater(advanced, 0)
        self.assertLess(advanced, self.ntables)

        # A deferred table keeps the old stable plus full ingest, so reads stay correct.
        oplog.check(self, session_follow, 0, pos)

        # Across a full period of pickups the round-robin advances every table exactly once, so the
        # updates over that window cover all tables.
        u0 = self.updated(session_follow)
        for _ in range(self.period):
            pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertEqual(self.updated(session_follow) - u0, self.ntables)

        oplog.check(self, session_follow, 0, pos)
        conn_follow.close()

    def test_selective_pickup_with_budget(self):
        # A generous budget rarely binds, so deferral still runs (via the aggregate-budget pre-pass
        # path rather than the inline path) and reads stay correct.
        oplog = Oplog()
        self.make_tables(self.session)
        thandles = [oplog.add_uri(self.uri(i)) for i in range(self.ntables)]

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() +
            self.conn_base_config + 'disaggregated=(role="follower",' +
            f'checkpoint_pickup_defer_period={self.period},' +
            'checkpoint_pickup_defer_min_kb=1048576,checkpoint_pickup_defer_budget_pct=50)')
        session_follow = conn_follow.open_session('')
        self.make_tables(session_follow)

        pos = 0
        for _ in range(3):
            pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertGreater(self.deferred(session_follow), 0)
        oplog.check(self, session_follow, 0, pos)
        conn_follow.close()

    def test_deferral_disabled_by_default(self):
        oplog = Oplog()
        self.make_tables(self.session)
        thandles = [oplog.add_uri(self.uri(i)) for i in range(self.ntables)]

        # No defer period configured: every table is picked up at every checkpoint.
        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        self.make_tables(session_follow)

        pos = 0
        for _ in range(4):
            pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertEqual(self.deferred(session_follow), 0)
        conn_follow.close()

    def test_reconfigure_disables_deferral(self):
        oplog, thandles, conn_follow, session_follow = self.setup_follower(self.period)

        # Two cycles (inserts then updates) to get deferrals going.
        pos = self.cycle(oplog, thandles, session_follow, conn_follow, 0)
        pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertGreater(self.deferred(session_follow), 0)

        # Turn deferral off on the fly; subsequent pickups defer nothing.
        conn_follow.reconfigure('disaggregated=(checkpoint_pickup_defer_period=0)')
        before = self.deferred(session_follow)
        for _ in range(3):
            pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertEqual(self.deferred(session_follow), before)
        conn_follow.close()

    def test_step_up_forces_full_pickup(self):
        oplog, thandles, conn_follow, session_follow = self.setup_follower(self.period)

        pos = self.cycle(oplog, thandles, session_follow, conn_follow, 0)
        pos = self.cycle(oplog, thandles, session_follow, conn_follow, pos)
        self.assertGreater(self.deferred(session_follow), 0)

        # Step the follower up. The step-up must fully pick up every deferred table before draining
        # ingest, so all data is present and correct once it is the leader.
        self.disagg_switch_follower_and_leader(conn_follow)
        oplog.check(self, session_follow, 0, pos)
        conn_follow.close()
