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
# test_layered_stepup17.py
#   A follower removes several keys of one table, some missing from the stable table, one already
#   deleted there and one live, and then steps up. The follower may first restart from the last
#   checkpoint, so the stable table is read back from disk, and write the removes again. The drain
#   reports every key with no value in the stable table in a single warning and in the statistic,
#   and reads stay correct through a checkpoint and a restart. The removes are written directly
#   into the ingest table as the value a follower remove stores.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup17(wttest.WiredTigerTestCase):
    table = __qualname__
    uri = f'layered:{table}'
    ingest_uri = f'file:{table}.wt_ingest'
    conn_base_config = ',create,statistics=(all),'
    skip_msg = 'step-up drain skipped'
    missing_keys = ['missing1', 'missing2', 'missing3']
    read_ts = (6, 9, 15, 22, 28)
    # The timestamps each key is visible at; every other key is never visible.
    found_at = {'deleted': [6], 'live': [6, 9, 15], 'other': list(read_ts)}

    disagg_storages = gen_disagg_storages(disagg_only=True)
    restarts = [
        ('no_restart', dict(restart=False)),
        ('restart', dict(restart=True)),
    ]
    scenarios = make_scenarios(disagg_storages, restarts)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def orphan_stat(self):
        c = self.session.open_cursor('statistics:')
        val = c[wiredtiger.stat.conn.disagg_step_up_ingest_drain_orphan_remove][2]
        c.close()
        return val

    def check_reads(self):
        c = self.session.open_cursor(self.uri)
        for ts in self.read_ts:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
            for key in self.missing_keys + ['deleted', 'live', 'other']:
                c.set_key(key)
                if ts in self.found_at.get(key, []):
                    self.assertEqual(c.search(), 0)
                    self.assertEqual(c.get_value(), b'v')
                else:
                    self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
            self.session.rollback_transaction()
        c.close()

    def write_removes(self):
        c = self.session.open_cursor(self.ingest_uri)
        self.session.begin_transaction()
        for key in self.missing_keys + ['deleted', 'live']:
            c[key] = b'\x14\x14'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        c.close()

    def test_stepup_remove_count(self):
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # Leader: one live key, one deleted key and an unrelated key.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for key in ('deleted', 'live', 'other'):
            c[key] = b'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        self.session.begin_transaction()
        c.set_key('deleted')
        self.assertEqual(c.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(8))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        self.conn.reconfigure('disaggregated=(role="follower")')
        self.write_removes()
        if self.restart:
            # The ingest table does not survive the restart, so write the removes again after it.
            checkpoint_meta = self.disagg_get_complete_checkpoint_meta()
            self.close_conn()
            self.open_conn(config=self.extensionsConfig() + self.conn_base_config +
                f'disaggregated=(role="follower",checkpoint_meta="{checkpoint_meta}")')
            self.write_removes()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        orphans = len(self.missing_keys) + 1
        before = self.orphan_stat()
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="leader")')
        output = self.readStdout()
        self.cleanStdout()
        self.assertEqual(output.count(self.skip_msg), 1)
        self.assertEqual(output.count(f'{self.uri}: step-up drain skipped {orphans} removes of '
            'keys with no value in the stable table'), 1)
        self.assertEqual(self.orphan_stat() - before, orphans)

        self.check_reads()
        self.session.checkpoint()
        self.check_reads()
        self.reopen_conn()
        self.check_reads()
