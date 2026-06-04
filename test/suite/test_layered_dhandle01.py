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

# test_layered_dhandle01.py
#   Regression for WT-17323: a follower's layered-table ingest btrees must be
#   reclaimed once their data is durable in the stable table.
#
# A follower never checkpoints its ingest btrees, so a written-to ingest btree
# stays dirty for its whole lifetime. The sweep server refuses to close dirty
# trees, so without a fix the ingest file descriptors accumulate monotonically -
# one per distinct table written - and are never reclaimed while the node stays a
# follower. The fix lets sweep discard a dirty ingest btree once every update it
# holds is durable in the stable table (max_ingest_write_ts <= last checkpoint
# timestamp); the data is then served from the stable component.

import os, sys, time
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_dhandle01(wttest.WiredTigerTestCase):
    conn_base_config = 'create,statistics=(all),' + \
        'file_manager=(close_idle_time=1,close_scan_interval=1,close_handle_minimum=0),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages('test_layered_dhandle01', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def open_follower(self):
        return self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',' + self.conn_config_follower)

    # Count the follower's open ingest-table file descriptors (its home is the
    # "follower/" subdirectory), optionally only those for a specific file name.
    # This is Linux only.
    def count_follower_ingest_fds(self, suffix='.wt_ingest'):
        n = 0
        for fd in os.listdir('/proc/self/fd'):
            try:
                target = os.readlink('/proc/self/fd/' + fd)
            except OSError:
                continue
            if target.endswith(suffix) and '/follower/' in target:
                n += 1
        return n

    # Advance the global checkpoint timestamp by writing to a churn table at
    # timestamps above start_ts and taking leader checkpoints the follower picks
    # up, until the predicate holds or we run out of patience. The churn must push
    # the checkpoint timestamp past each idle table's max_ingest_write_ts.
    def churn_until(self, churn_uri, sfollow, conn_follow, start_ts, predicate):
        ts = start_ts
        for _ in range(40):
            cl = self.session.open_cursor(churn_uri)
            cf = sfollow.open_cursor(churn_uri)
            for _ in range(300):
                ts += 1
                with self.transaction(commit_timestamp=ts):
                    cl[ts] = 'churn'
                with self.transaction(session=sfollow, commit_timestamp=ts):
                    cf[ts] = 'churn'
            cl.close()
            cf.close()
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts))
            self.session.checkpoint()
            self.disagg_advance_checkpoint(conn_follow)
            time.sleep(1)
            if predicate():
                break
        return ts

    def test_ingest_fd_reclaimed(self):
        if not sys.platform.startswith('linux'):
            self.skipTest('ingest fd counting uses /proc, Linux only')

        ntables = 60
        conn_follow = self.open_follower()
        sfollow = conn_follow.open_session('')
        oplog = Oplog()

        tables = []
        for i in range(ntables):
            uri = f'layered:dh01_{i:05d}'
            self.session.create(uri, 'key_format=S,value_format=S')
            sfollow.create(uri, 'key_format=S,value_format=S')
            tables.append((uri, oplog.add_uri(uri)))

        # Write a little to every table, then leave them all idle. Apply to the
        # leader (durable in stable after checkpoint) and to the follower (ingest).
        for (_, t) in tables:
            oplog.insert(t, 5)
        apply_pos = len(oplog._entries)
        oplog.apply(self, self.session, 0, apply_pos)
        oplog.apply(self, sfollow, 0, apply_pos)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Every table now has an open, dirty ingest btree on the follower.
        self.assertGreaterEqual(self.count_follower_ingest_fds(), ntables)

        # Drive the global checkpoint timestamp past every table's
        # max_ingest_write_ts; sweep then discards the now-durable ingest btrees.
        self.session.create('layered:dh01_churn', 'key_format=i,value_format=S')
        sfollow.create('layered:dh01_churn', 'key_format=i,value_format=S')
        self.churn_until('layered:dh01_churn', sfollow, conn_follow,
            oplog.last_timestamp(), lambda: self.count_follower_ingest_fds() <= 5)

        # The idle ingest btrees must be reclaimed: only a small active set (the
        # churn table and any in-flight handles) remains, not one per table.
        self.assertLessEqual(self.count_follower_ingest_fds(), 5,
            'follower ingest file descriptors were not reclaimed')

        # No data was lost: every key reads back on the follower, served from the
        # stable component now that the ingest btrees are gone.
        oplog.check(self, sfollow, 0, len(oplog._entries))

        sfollow.close()
        conn_follow.close()

    def test_truncate_durable_reclaim(self):
        if not sys.platform.startswith('linux'):
            self.skipTest('ingest fd counting uses /proc, Linux only')

        uri = 'layered:dh01_trunc'
        nrows = 1000
        lo, hi = 100, 700      # truncate removes keys [lo, hi] inclusive

        conn_follow = self.open_follower()
        sfollow = conn_follow.open_session('')

        self.session.create(uri, 'key_format=i,value_format=S')
        sfollow.create(uri, 'key_format=i,value_format=S')

        # Apply identical operations on leader (-> stable) and follower (-> ingest
        # plus the layered dhandle's truncate list): write nrows, then truncate a
        # range. The follower's truncate entry is the state we must not lose when
        # its ingest btree is later discarded.
        def write_and_truncate(session):
            with self.transaction(session=session, commit_timestamp=nrows):
                c = session.open_cursor(uri)
                for i in range(nrows):
                    c[i] = 'value' + str(i)
                c.close()

            c_start = session.open_cursor(uri)
            c_start.set_key(lo)
            c_stop = session.open_cursor(uri)
            c_stop.set_key(hi)

            with self.transaction(session=session, commit_timestamp=nrows + 1):
                session.truncate(None, c_start, c_stop, None)

            c_start.close()
            c_stop.close()

        write_and_truncate(self.session)
        write_and_truncate(sfollow)

        # Make the leader's data and truncate durable, and let the follower pick it
        # up so its own ingest copy becomes redundant.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(nrows + 1)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        expected = nrows - (hi - lo + 1)   # surviving rows [0,100) and (700,1000)

        def follower_row_count():
            c = sfollow.open_cursor(uri)
            count = 0
            while c.next() == 0:
                count += 1
            c.close()
            return count

        # Churn with checkpoints so sweep eventually discards its ingest btree on the follower.
        ingest = 'dh01_trunc.wt_ingest'
        self.session.create('layered:dh01_churn', 'key_format=i,value_format=S')
        sfollow.create('layered:dh01_churn', 'key_format=i,value_format=S')
        self.churn_until('layered:dh01_churn', sfollow, conn_follow,
            nrows + 1, lambda: self.count_follower_ingest_fds(ingest) == 0)

        self.assertEqual(self.count_follower_ingest_fds(ingest), 0,
            'follower ingest btree with a pending truncate was not reclaimed')

        # The truncate must still be reflected after the ingest btree is gone: the
        # range is absent and the rest is present, served from the stable component.
        self.assertEqual(follower_row_count(), expected)

        # Step up: the drain replays the truncate from the layered dhandle's list
        # against stable, with the ingest btree already discarded. Data must remain
        # correct.
        conn_follow.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(follower_row_count(), expected)

        sfollow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
