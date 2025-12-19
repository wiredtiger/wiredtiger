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

import time, wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
@disagg_test_class
class test_layered68(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages('test_layered68', disagg_only = True)

    uri = "layered:test_layered68"

    # Test simple inserts to a leader/follower
    def dbg(self, s):
        self.tty(s)  # debug TODO
        self.pr(s)

    def evict_entire_btree(self, session, uri):
        evict_cursor = session.open_cursor(uri, None, "debug=(release_evict)")
        while evict_cursor.next() == 0:
            _ = evict_cursor.get_key()
            _ = evict_cursor.get_value()
        evict_cursor.close()

    def test_leader_follower(self):
        # Create the table on leader and tell oplog about it
        session = self.session
        session.create(self.uri, "key_format=S,value_format=S,block_manager=disagg")

        # Create the follower and create its table
        # To keep this test relatively easy, we're only using a single URI.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S,block_manager=disagg")
        session_follow.create("table:local", "key_format=S,value_format=S")

        lc = session.open_cursor(self.uri)
        fc = session_follow.open_cursor(self.uri)

        local_cur = session_follow.open_cursor("table:local")
        limit = 8001  # If set high > 12000, get a different crash.
        last_ckpt = 0
        last_advance = 0
        oldest = 0
        stable = 0;
        for ts in range(1, limit):
            if ts % 1000 == 0:
                self.dbg(f'timestamp {ts}/{limit - 1}')
                fc.close()
                fc = session_follow.open_cursor(self.uri)

            # Occasionally set the oldest timestamp at around 90% of the current timestamp
            # to let history age out.  We don't let set the oldest timestamp on the follower,
            # we'll force it to consult multiple history tables.
            if ts % 101 == 0:
                proposed_oldest = int(ts * 0.9) - 100
                if proposed_oldest > oldest and proposed_oldest < stable:
                    oldest = proposed_oldest
                    self.dbg(f'oldest_timestamp={oldest}')
                    self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest))

            # At a slightly different pace, force evict the data file to put stuff into the history
            # (or age it out)
            if ts % 121 == 0:
                self.evict_entire_btree(session, self.uri)

            # Do checkpoints with gaps between them roughly doubling each time.
            if ts > last_ckpt * 2 and ts > 100:
                # We set the stable timestamp near the checkpoint, with some variability.
                proposed_stable = ts - 99 + (last_ckpt + ts) % 97
                if proposed_stable >= oldest:
                    stable = proposed_stable
                    self.dbg(f'stable_timestamp={stable}')
                    self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(stable))
                last_ckpt = ts
                self.dbg(f'checkpoint at {ts}')
                session.checkpoint()

            # Advance the follower's checkpoint after enough data has accumulated
            if last_ckpt != 0 and ts == last_ckpt + 533:
                self.dbg(f'advance follower at {ts}')
                self.disagg_advance_checkpoint(conn_follow)
                last_advance = ts

            for is_follower in [False, True]:
                if is_follower:
                    cur = fc
                else:
                    cur = lc

                cur.session.begin_transaction()
                if ts % 13 == 0:
                    cur.set_key('a')
                    cur.remove()
                else:
                    cur['a'] = str(ts) + 'a' * 1000

                if ts % 19 == 0:
                    cur.set_key('b')
                    cur.remove()
                else:
                    cur['b'] = str(ts) + 'b' * 1000

                cur.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

                if ts > 1000:
                    # TODO
                    # FIXME-WT-16292 this test does not succeed on the follower yet,
                    # due to history store correctness errors.  Still, on the follower,
                    # go through the motions so we are accessing the history store,
                    # just don't check the results.
                    broken = is_follower
                    broken = False

                    # Look back to timestamp minus a thousand and make sure
                    # we can still see the correct value
                    oldts = ts - 1000
                    if not is_follower:
                        oldts = max(oldts, oldest)

                    try:
                        with self.transaction(session = cur.session, read_timestamp = oldts, rollback = True):
                            cur.set_key('a')
                            result = cur.search()
                            if oldts % 13 == 0:
                                if not broken:
                                    self.assertEqual(result, wiredtiger.WT_NOTFOUND)
                            else:
                                expecta  = str(oldts) + 'a' * 1000
                                if not broken:
                                    self.assertEqual(result, 0)
                                    self.assertEqual(cur.get_value(), expecta)
                            cur.set_key('b')
                            result = cur.search()
                            if oldts % 19 == 0:
                                self.assertEqual(result, wiredtiger.WT_NOTFOUND)
                            else:
                                expecta  = str(oldts) + 'b' * 1000
                                if not broken:
                                    self.assertEqual(result, 0)
                                    self.assertEqual(cur.get_value(), expecta)
                            cur.set_key('NO')
                            self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
                    except:
                        where = "follower" if is_follower else "leader"
                        self.dbg(f'error occurred on {where} ts={oldts}')
                        raise

            if ts % 100 == 0:
                cur.session.begin_transaction()
                if ts % 7 == 0:
                    local_cur.set_key('c')
                    local_cur.remove()
                else:
                    local_cur['c'] = str(ts) + 'c' * 1000
                cur.session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

            # Every so often, make sure the history table is evicted.
            if ts % 700:
                self.evict_entire_btree(session_follow, "file:WiredTigerHS.wt")
                self.evict_entire_btree(session_follow, "file:WiredTigerSharedHS.wt_stable")
                
        lc.close()
        fc.close()
