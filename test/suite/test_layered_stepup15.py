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
#
# test_layered_stepup15.py
#   A follower removes a key, possibly more than once, and then steps up. The key may be missing
#   from the stable table, already deleted there, or live. Removes of a key with no value in the
#   stable table are counted and reported once per table, and the node keeps running through a
#   checkpoint and a restart. The removes are written as the layered delete marker directly into
#   the ingest table, which is what a follower remove stores.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup15(wttest.WiredTigerTestCase):
    table = __qualname__
    uri = f'layered:{table}'
    ingest_uri = f'file:{table}.wt_ingest'
    warning = 'step-up drain skipped 1 removes of keys with no value in the stable table'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    stable_states = [
        ('absent', dict(stable_state='absent', remove_ts=[20])),
        ('absent_stacked', dict(stable_state='absent', remove_ts=[20, 25])),
        ('deleted', dict(stable_state='deleted', remove_ts=[20])),
        ('present', dict(stable_state='present', remove_ts=[20])),
        ('present_stacked', dict(stable_state='present', remove_ts=[20, 25])),
    ]
    scenarios = make_scenarios(disagg_storages, stable_states)

    def conn_config(self):
        return self.extensionsConfig() + ',create,statistics=(all),disaggregated=(role="leader")'

    def orphan_stat(self):
        c = self.session.open_cursor('statistics:')
        val = c[wiredtiger.stat.conn.disagg_step_up_ingest_drain_orphan_remove][2]
        c.close()
        return val

    def search_k(self, ts=None):
        if ts is not None:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        c = self.session.open_cursor(self.uri)
        c.set_key('k')
        ret = c.search()
        c.close()
        if ts is not None:
            self.session.rollback_transaction()
        return ret

    def test_stepup_remove(self):
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # Leader: write the stable state of the key.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['other'] = b'v'
        if self.stable_state != 'absent':
            c['k'] = b'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        if self.stable_state == 'deleted':
            self.session.begin_transaction()
            c.set_key('k')
            self.assertEqual(c.remove(), 0)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(8))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # Follower: write each remove into the ingest table.
        self.conn.reconfigure('disaggregated=(role="follower")')
        c = self.session.open_cursor(self.ingest_uri)
        for ts in self.remove_ts:
            self.session.begin_transaction()
            c['k'] = b'\x14\x14'
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c.close()

        # Let checkpoints include the removes while readers can still read before them.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        orphan = self.stable_state != 'present'
        before = self.orphan_stat()
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="leader")')
        output = self.readStdout()
        self.cleanStdout()
        self.assertEqual(output.count(self.warning), 1 if orphan else 0)
        self.assertEqual(self.orphan_stat() - before, 1 if orphan else 0)

        if self.stable_state == 'present':
            self.assertEqual(self.search_k(15), 0)
            for ts in (22, 28):
                self.assertEqual(self.search_k(ts), wiredtiger.WT_NOTFOUND)

        # Write the stable page holding the key, then restart.
        self.session.checkpoint()
        self.assertEqual(self.search_k(), wiredtiger.WT_NOTFOUND)
        self.reopen_conn()
        self.assertEqual(self.search_k(), wiredtiger.WT_NOTFOUND)
        c = self.session.open_cursor(self.uri)
        self.assertEqual(c['other'], b'v')
        c.close()
