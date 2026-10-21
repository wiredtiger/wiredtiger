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
# test_layered_stepup16.py
#   A prepared transaction on a follower removes a key with no value in the stable table and is
#   still unresolved when the follower steps up. The remove is skipped, counted and reported. The
#   transaction then commits or rolls back on the new leader, and reads stay correct through a
#   checkpoint and a restart. The remove is written directly into the ingest table as the value a
#   follower remove stores.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup16(wttest.WiredTigerTestCase):
    table = __qualname__
    uri = f'layered:{table}'
    ingest_uri = f'file:{table}.wt_ingest'
    warning = f'layered:{table}: step-up drain skipped 1 remove(s) of keys with no value in the ' \
        'stable table'
    read_ts = (6, 9, 15, 22, 28)

    disagg_storages = gen_disagg_storages(disagg_only=True)
    stable_states = [
        ('absent', dict(stable_state='absent')),
        ('deleted', dict(stable_state='deleted')),
    ]
    resolutions = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    scenarios = make_scenarios(disagg_storages, stable_states, resolutions)

    def conn_config(self):
        return self.extensionsConfig() + ',create,statistics=(all),precise_checkpoint=true,' \
            'preserve_prepared=true,disaggregated=(role="leader")'

    def orphan_stat(self):
        c = self.session.open_cursor('statistics:')
        val = c[wiredtiger.stat.conn.disagg_step_up_ingest_drain_orphan_remove][2]
        c.close()
        return val

    def check_reads(self):
        found = {6: b'v'} if self.stable_state == 'deleted' else {}
        session = self.conn.open_session()
        for ts in self.read_ts:
            session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
            c = session.open_cursor(self.uri)
            c.set_key('k')
            ret = c.search()
            if ts in found:
                self.assertEqual((ret, c.get_value()), (0, found[ts]))
            else:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
            c.close()
            session.rollback_transaction()
        session.close()

    def test_stepup_prepared_remove(self):
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # Leader: write the stable state of the key.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['other'] = b'v'
        if self.stable_state == 'deleted':
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

        # Follower: prepare a transaction that removes the key.
        self.conn.reconfigure('disaggregated=(role="follower")')
        prepared = self.conn.open_session()
        c = prepared.open_cursor(self.ingest_uri)
        prepared.begin_transaction()
        c['k'] = b'\x14\x14'
        c.close()
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.prepared_id_str(1))

        before = self.orphan_stat()
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="leader")')
        output = self.readStdout()
        self.cleanStdout()
        self.assertEqual(output.count(self.warning), 1)
        self.assertEqual(self.orphan_stat() - before, 1)

        # Resolve the transaction on the new leader.
        if self.commit:
            prepared.timestamp_transaction('commit_timestamp=' + self.timestamp_str(25) +
                ',durable_timestamp=' + self.timestamp_str(26))
            prepared.commit_transaction()
        else:
            prepared.rollback_transaction('rollback_timestamp=' + self.timestamp_str(25))
        prepared.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        self.check_reads()
        self.session.checkpoint()
        self.check_reads()
        self.reopen_conn()
        self.check_reads()
