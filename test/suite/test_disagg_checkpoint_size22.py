#!/usr/bin/env python
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
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog

# A follower must report the same block_size for a layered table as the leader (the last
# checkpoint's size, read from checkpoint metadata), regardless of statistics configuration.
@disagg_test_class
class test_disagg_checkpoint_size22(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),' + \
        'statistics_log=(wait=0,json=true,on_close=true),'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages(disagg_only = True)

    uri = f"layered:{test_name}"
    nitems = 20000
    stat_configs = ('statistics=(fast)', 'statistics=(size)', 'statistics=(all)',
        'statistics=(cache_walk)', 'statistics=(tree_walk)')

    def block_size(self, session, config):
        cstat = session.open_cursor('statistics:' + self.uri, None, config)
        sz = cstat[stat.dsrc.block_size][2]
        cstat.close()
        return sz

    # All statistics cursor types (fast/size, which read checkpoint metadata directly, and
    # all/cache_walk/tree_walk, which additionally open and walk the stable checkpoint) must agree
    # on block_size, and leader and follower must agree with each other.
    def test_all_stat_types_report_same_block_size(self):
        oplog = Oplog()
        self.session.create(self.uri, "key_format=S,value_format=S")
        t = oplog.add_uri(self.uri)

        conn_follow = self.wiredtiger_open(
            'follower', self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, "key_format=S,value_format=S")

        # Leader writes and checkpoints; follower applies the same oplog and picks up the checkpoint.
        oplog.insert(t, self.nitems)
        oplog.apply(self, self.session, 0, self.nitems)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()
        oplog.apply(self, session_follow, 0, self.nitems)
        self.disagg_advance_checkpoint(conn_follow)

        leader_sizes = {c: self.block_size(self.session, c) for c in self.stat_configs}
        follow_sizes = {c: self.block_size(session_follow, c) for c in self.stat_configs}

        self.assertEqual(len(set(leader_sizes.values())), 1,
            f"leader block_size should agree across statistics types: {leader_sizes}")
        self.assertEqual(len(set(follow_sizes.values())), 1,
            f"follower block_size should agree across statistics types: {follow_sizes}")

        leader_size = next(iter(leader_sizes.values()))
        follow_size = next(iter(follow_sizes.values()))
        self.assertGreater(follow_size, 4096,
            "follower block_size should reflect the checkpoint, not the empty ingest base")
        self.assertEqual(follow_size, leader_size,
            f"follower block_size ({follow_size}) should match leader block_size ({leader_size})")

        session_follow.close()
        conn_follow.close()
