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

# test_layered_arm01.py
#    A planned step-down with a straddler: a write transaction that began before the arm commits
#    after it, into stable alone. The demotion is refused until a checkpoint covers that commit,
#    then every commit, straddling or armed, is readable on the demoted node, on the successor once
#    it applies the same writes and takes the lead, and on the demoted node again once it adopts
#    the successor's checkpoint.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm01(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_arm01'

    def rows(self, tag, n=20):
        return {f'{tag}{i:03}': tag for i in range(n)}

    def test_straddler_then_demote(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        pre = self.rows('pre')
        self.write_at(self.uri, pre, 10)
        self.checkpoint_at(10)

        # The straddler begins before the arm and commits after it.
        straddler = self.conn.open_session()
        cursor = straddler.open_cursor(self.uri)
        straddler.begin_transaction()
        straddled = self.rows('s')
        for k, v in straddled.items():
            cursor[k] = v
        self.arm()
        straddler.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        straddler.close()

        armed = self.rows('a')
        self.write_at(self.uri, armed, 40)

        # The straddler wrote stable alone; the armed transaction mirrored into ingest.
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 50), set(armed))
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 50),
            set(pre) | set(straddled) | set(armed))

        # No checkpoint yet covers the straddler's commit, so the demotion is refused and the node
        # stays a working leader.
        for ckpt_ts in (None, 20):
            if ckpt_ts is not None:
                self.checkpoint_at(ckpt_ts)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.conn.reconfigure('disaggregated=(role="follower")'),
                '/step-down refused: last checkpoint .* is below the newest unmirrored commit/')
            self.assertEqual(self.conn_stat(stat.conn.disagg_role_leader), 1)
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_refused_plain_high), 2)
        self.assertEqual(self.conn_stat(stat.conn.disagg_plain_high), 30)

        # A checkpoint at the straddler's commit is enough; the armed commit above it is in ingest.
        self.checkpoint_at(30)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.conn_stat(stat.conn.disagg_role_leader), 0)
        self.assertEqual(self.conn_stat(stat.conn.disagg_step_down_armed), 0)

        everything = {**pre, **straddled, **armed}
        self.assertEqual(self.read_kvs(self.uri), everything)
        self.assertEqual(self.read_kvs_at(self.uri, 35), {**pre, **straddled})

        # The successor picks up the checkpoint, applies the armed writes as replication would, and
        # takes the lead.
        conn_b = self.open_node('node_b', config=self.conn_base_config)
        conn_b.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(30))
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.assertEqual(self.read_kvs(self.uri, session_b), {**pre, **straddled})
        self.write_at(self.uri, armed, 40, session_b)
        self.promote(conn_b)
        self.assertEqual(self.read_kvs(self.uri, session_b), everything)

        later = self.rows('b')
        self.write_at(self.uri, later, 50, session_b)
        self.checkpoint_at(50, conn_b)

        # The demoted node adopts the successor's checkpoint and serves every commit.
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        self.assertEqual(self.read_kvs(self.uri), {**everything, **later})

        session_b.close()
        conn_b.close('debug=(skip_checkpoint=true)')

if __name__ == '__main__':
    wttest.run()
