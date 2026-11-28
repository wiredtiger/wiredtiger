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
# A follower's layered-table ingest btrees can be reclaimed once their data
# is durable in the stable table.
#
# A follower never checkpoints its ingest btrees, so an ingest btree, once
# written to, stays dirty forever. Sweep can still close it once the follower
# picks up a checkpoint covering its newest durable change.

import os, time
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_dhandle01(wttest.WiredTigerTestCase):
    conn_base_config = 'create,statistics=(all),' + \
        'file_manager=(close_idle_time=1,close_scan_interval=1,close_handle_minimum=0),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)
    sweep_timeout = 60

    # Enough passes for sweep to set a time of death and wait out the idle time.
    sweep_passes = 5

    def open_follower(self):
        return self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',' + self.conn_config_follower)

    def get_stat(self, session, stat):
        c = session.open_cursor('statistics:')
        value = c[stat][2]
        c.close()
        return value

    def wait_for_stat(self, session, stat, minimum, msg):
        deadline = time.time() + self.sweep_timeout
        while self.get_stat(session, stat) < minimum:
            if time.time() > deadline:
                self.fail(f'{msg} within {self.sweep_timeout}s')
            time.sleep(0.1)

    # Sweep must not close an ingest btree the follower's latest checkpoint does not cover.
    def assert_no_ingest_reclaim(self, session):
        sweeps = self.get_stat(session, wiredtiger.stat.conn.dh_sweeps)
        self.wait_for_stat(session, wiredtiger.stat.conn.dh_sweeps, sweeps + self.sweep_passes,
            'sweep server did not run')
        self.assertEqual(self.get_stat(session, wiredtiger.stat.conn.dh_sweep_ingest_close), 0,
            'sweep closed an ingest btree that is not yet durable')

    def assert_no_ingest_file(self, home, name):
        path = os.path.join(home, name) if home else name
        self.assertFalse(os.path.exists(path), path + ' should not exist')

    def test_ingest_reclaimed(self):
        ntables = 5
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

        # Create does not leave a file behind. The metadata entry is the object.
        self.assert_no_ingest_file('', 'dh01_00000.wt_ingest')
        self.assert_no_ingest_file('follower', 'dh01_00000.wt_ingest')

        self.assert_no_ingest_reclaim(sfollow)

        # After pickup every ingest btree is redundant.
        self.disagg_advance_checkpoint_and_wait(conn_follow)
        self.wait_for_stat(sfollow, wiredtiger.stat.conn.dh_sweep_ingest_close, ntables,
            'follower ingest btrees were not reclaimed')

        # No data was lost: every key reads back on the follower, served from the
        # stable component now that the ingest btrees are gone.
        oplog.check(self, sfollow, 0, len(oplog._entries))

        sfollow.close()
        conn_follow.close()

    def test_truncate_durable_reclaim(self):
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

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(nrows + 1)}')
        self.session.checkpoint()

        expected = nrows - (hi - lo + 1)   # surviving rows [0,100) and (700,1000)

        def follower_row_count():
            c = sfollow.open_cursor(uri)
            count = 0
            while c.next() == 0:
                count += 1
            c.close()
            return count

        self.assert_no_ingest_file('', 'dh01_trunc.wt_ingest')
        self.assert_no_ingest_file('follower', 'dh01_trunc.wt_ingest')

        self.assert_no_ingest_reclaim(sfollow)

        # After pickup the follower's ingest btree is redundant.
        self.disagg_advance_checkpoint_and_wait(conn_follow)
        self.wait_for_stat(sfollow, wiredtiger.stat.conn.dh_sweep_ingest_close, 1,
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

    def read_value(self, uri, key, read_ts):
        cursor = self.session.open_cursor(uri)
        with self.transaction(read_timestamp=read_ts, rollback=True):
            cursor.set_key(key)
            value = cursor.get_value() if cursor.search() == 0 else None
        cursor.close()
        return value

    # A transaction prepares before a planned step-down is announced and resolves after it. The
    # step-down checkpoint is older than that resolution, so the demoted node must keep it until
    # a later checkpoint covers it.
    def check_prepared_resolution_survives_step_down(self, commit):
        uri = 'layered:dh01_prepared'
        ts = self.timestamp_str

        self.reopen_conn(config=self.conn_base_config +
            'preserve_prepared=true,precise_checkpoint=true,disaggregated=(role="leader")')
        self.conn.set_timestamp(f'oldest_timestamp={ts(1)},stable_timestamp={ts(1)}')
        self.session.create(uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(uri)
        with self.transaction(commit_timestamp=2):
            cursor['key'] = 'old'
        self.conn.set_timestamp(f'stable_timestamp={ts(3)}')
        self.session.checkpoint()

        # Write as a follower, and keep the ingest table open so it stays in use through the
        # leader era that follows.
        self.conn.reconfigure('disaggregated=(role="follower")')
        ingest_session = self.conn.open_session('')
        ingest_cursor = ingest_session.open_cursor('file:dh01_prepared.wt_ingest')
        with self.transaction(commit_timestamp=5):
            cursor['follower-key'] = 'value'
        cursor.close()
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Prepare, announce the step-down, then resolve after the announced timestamp.
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        prepare_cursor['key'] = 'new'
        prepare_session.prepare_transaction(
            f'prepare_timestamp={ts(10)},prepared_id={self.prepared_id_str(1)}')
        self.conn.set_timestamp(f'step_down_timestamp={ts(11)}')
        if commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={ts(13)},durable_timestamp={ts(13)}')
        else:
            prepare_session.rollback_transaction(f'rollback_timestamp={ts(13)}')
        prepare_cursor.close()
        prepare_session.close()
        expected = 'new' if commit else 'old'

        self.conn.set_timestamp(f'stable_timestamp={ts(11)}')
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')

        # The ingest table is now idle, but the checkpoint does not cover the resolution.
        ingest_cursor.close()
        ingest_session.close()
        self.assert_no_ingest_reclaim(self.session)
        self.assertEqual(self.read_value(uri, 'key', 20), expected)

        # The resolution carries over a step-up.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.read_value(uri, 'key', 20), expected)

        # A checkpoint covering the resolution makes the ingest table redundant.
        self.conn.set_timestamp(f'stable_timestamp={ts(20)}')
        self.session.checkpoint()
        self.wait_for_stat(self.session, wiredtiger.stat.conn.dh_sweep_ingest_close, 1,
            'ingest btree was not reclaimed once the checkpoint covered the resolution')
        self.assertEqual(self.read_value(uri, 'key', 20), expected)

    def test_prepared_commit_survives_step_down(self):
        self.check_prepared_resolution_survives_step_down(commit=True)

    def test_prepared_rollback_survives_step_down(self):
        self.check_prepared_resolution_survives_step_down(commit=False)
