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

import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wiredtiger import stat

# test_layered23.py
#    Test the basic ability to insert on a follower.

@disagg_test_class
class test_layered23(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages('test_layered23', disagg_only = True)

    uri = "layered:test_layered23"

    # Make sure the stats agree that the leader has done each checkpoint.
    def check_checkpoint(self, expected):
        stat_cur = self.session.open_cursor('statistics:', None, None)
        self.assertEqual(stat_cur[stat.conn.checkpoints_total_succeed][2], expected)
        stat_cur.close()

    def show_version_cursor(self, session, label, uri, key):
        self.tty(f'Python Version Cursor for {label} ({uri})')
        config = "debug=(dump_version=(enabled=true))"
        vcursor = session.open_cursor(uri, None, config)
        vcursor.set_key(key)
        self.assertEqual(vcursor.search(), 0)
        while True:
            self.assertEqual(vcursor.get_key(), key)
            v = vcursor.get_values()
            line = f'txnid={v[0]}, ' + \
                f'start_ts={v[1]}, ' + \
                f'start_durable_ts={v[2]}, ' + \
                f'stop_ts={v[4]}, ' + \
                f'stop_durable_ts={v[5]}, ' + \
                f'type={v[6]}, ' + \
                f'prepare_state={v[7]}, ' + \
                f'flags={v[8]}, ' + \
                f'location={v[9]}, ' + \
                f'value={v[10]}'
            self.tty('  ' + line)
            self.pr('  ' + line)
            ret = vcursor.next()
            if ret == wiredtiger.WT_NOTFOUND:
                break
            self.assertEqual(ret, 0)
        self.tty(' ')
        vcursor.close()

    # Test simple inserts to a leader/follower
    def test_leader_follower(self):
        # Create the oplog
        oplog = Oplog()

        # Create the table on leader and tell oplog about it
        self.session.create(self.uri, "key_format=S,value_format=S")
        t = oplog.add_uri(self.uri)

        # Create the follower and create its table
        # To keep this test relatively easy, we're only using a single URI.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Create some oplog traffic, a mix of inserts and updates
        oplog.insert(t, 900)
        oplog.update(t, 100)
        oplog.insert(t, 900)
        oplog.update(t, 100)

        # Apply them to leader WT and checkpoint.
        oplog.apply(self, self.session, 0, 2000)
        oplog.check(self, self.session, 0, 2000)

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')

        self.session.checkpoint()     # checkpoint 1
        self.show_version_cursor(self.session, "checkpoint 1 leader", 'file:test_layered23.wt_stable', "1")
        checkpoint_count = 1
        self.check_checkpoint(checkpoint_count)

        # Add some more traffic
        oplog.insert(t, 900)
        oplog.update(t, 100)
        oplog.apply(self, self.session, 2000, 1000)
        oplog.check(self, self.session, 0, 3000)

        # On the follower -
        # Apply some entries, a bit more than checkpoint 1
        oplog.apply(self, session_follow, 0, 2100)

        self.pr(f'OPLOG: {oplog}')
        oplog.check(self, session_follow, 0, 2100)

        # Then advance the checkpoint and make sure everything is still good
        self.pr('advance checkpoint')
        self.disagg_advance_checkpoint(conn_follow)
        self.show_version_cursor(session_follow, "checkpoint 1 follower ingest",
                                 f'file:test_layered23.wt_ingest', "1")
        self.show_version_cursor(session_follow, "checkpoint 1 follower stable",
                                 f'file:test_layered23.wt_stable/WiredTigerCheckpoint.1', "1")
        oplog.check(self, session_follow, 0, 2100)

        # Now go back to leader, checkpoint and insert more.
        # On follower apply some, advance.
        # Rinse and repeat.
        leader_pos = 3000
        follower_pos = 2100

        for i in range(1, 10):
            self.tty(f'iteration {i}, setting stable_timestamp to {oplog.last_timestamp()}')
            self.pr(f'iteration {i}')
            self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
            self.conn.set_timestamp(f'oldest_timestamp=1')

            self.session.checkpoint()
            self.show_version_cursor(self.session, f"checkpoint {i+1} leader", 'file:test_layered23.wt_stable', "1")
            checkpoint_pos = leader_pos
            checkpoint_count += 1
            self.check_checkpoint(checkpoint_count)

            # Every few times have no data between checkpoints.
            if i % 3 != 0:
                # More traffic on leader, stay ahead because follower will advance past checkpoint
                # before picking up checkpoint.
                oplog.insert(t, 900)
                oplog.update(t, 100)
                oplog.apply(self, self.session, leader_pos, 1000)
                leader_pos += 1000

                # The check begins at 0, which means this test will have quadratic performance.
                # We don't always have to start at 0.
                oplog.check(self, self.session, 0, leader_pos)

            # On follower, apply oplog. Stay a little behind the leader, but
            # we always must be in front of the checkpoint.
            follower_new_pos = max(min(follower_pos, leader_pos - 900), checkpoint_pos)
            to_apply = follower_new_pos - follower_pos
            oplog.apply(self, session_follow, follower_pos, to_apply)
            follower_pos = follower_new_pos

            self.show_version_cursor(session_follow, f"checkpoint {i} follower ingest before pickup",
                                     f'file:test_layered23.wt_ingest', "1")
            self.show_version_cursor(session_follow, f"checkpoint {i} follower stable before pickup",
                                     f'file:test_layered23.wt_stable/WiredTigerCheckpoint.{i}', "1")

            if i == 2:
                self.tty('\n\n!!! STOP HERE!!!\n\n')

            self.tty(f'checking follower from pos 0 to {follower_pos} before checkpoint pick-up')
            self.pr(f'checking follower from pos 0 to {follower_pos} before checkpoint pick-up')
            oplog.check(self, session_follow, 0, follower_pos)

            # advance checkpoint
            self.tty('advance checkpoint')
            self.pr('advance checkpoint')
            self.disagg_advance_checkpoint(conn_follow)
            self.show_version_cursor(session_follow, f"checkpoint {i+1} follower ingest",
                                     f'file:test_layered23.wt_ingest', "1")
            self.show_version_cursor(session_follow, f"checkpoint {i+1} follower stable",
                                     f'file:test_layered23.wt_stable/WiredTigerCheckpoint.{i+1}', "1")

            # The check begins at 0, which means this test will have quadratic performance.
            self.tty(f'checking follower from pos 0 to {follower_pos} after checkpoint pick-up')
            self.pr(f'checking follower from pos 0 to {follower_pos} after checkpoint pick-up')
            oplog.check(self, session_follow, 0, follower_pos)
