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
#   from the stable table, already deleted there, or live; a newer insert may follow the removes,
#   and other keys missing from the stable table may be removed too. Removes of keys with no value
#   in the stable table are skipped, counted and reported with one warning per table, reads at
#   every timestamp stay correct, and the node keeps running through a checkpoint and a restart.
#   The removes are written directly into the ingest table as the value a follower remove stores.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup15(wttest.WiredTigerTestCase):
    table = __qualname__
    uri = f'layered:{table}'
    ingest_uri = f'file:{table}.wt_ingest'
    live_uri = f'layered:{table}_live'
    live_ingest_uri = f'file:{table}_live.wt_ingest'
    skip_msg = 'step-up drain skipped'
    read_ts = (6, 9, 15, 22, 28)

    disagg_storages = gen_disagg_storages(disagg_only=True)
    stable_states = [
        ('absent', dict(stable_state='absent', remove_ts=[20], insert_ts=None, found_at=[])),
        ('absent_multiple', dict(stable_state='absent', remove_ts=[20], insert_ts=None, found_at=[],
            extra_keys=['missing1', 'missing2', 'missing3'])),
        ('absent_stacked',
            dict(stable_state='absent', remove_ts=[20, 25], insert_ts=None, found_at=[])),
        ('absent_then_insert',
            dict(stable_state='absent', remove_ts=[20], insert_ts=25, found_at=[28])),
        ('deleted', dict(stable_state='deleted', remove_ts=[20], insert_ts=None, found_at=[6])),
        ('present',
            dict(stable_state='present', remove_ts=[20], insert_ts=None, found_at=[6, 9, 15])),
        ('present_stacked',
            dict(stable_state='present', remove_ts=[20, 25], insert_ts=None, found_at=[6, 9, 15])),
    ]
    scenarios = make_scenarios(disagg_storages, stable_states)

    def conn_config(self):
        return self.extensionsConfig() + ',create,statistics=(all),disaggregated=(role="leader")'

    def orphan_stat(self):
        c = self.session.open_cursor('statistics:')
        val = c[wiredtiger.stat.conn.disagg_step_up_ingest_drain_orphan_remove][2]
        c.close()
        return val

    def search(self, uri, ts=None, key='k'):
        if ts is not None:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        c = self.session.open_cursor(uri)
        c.set_key(key)
        ret = c.search()
        value = c.get_value() if ret == 0 else None
        c.close()
        if ts is not None:
            self.session.rollback_transaction()
        return ret, value

    def check_reads(self):
        expect_value = b'w' if self.insert_ts else b'v'
        for ts in self.read_ts:
            ret, value = self.search(self.uri, ts)
            if ts in self.found_at:
                self.assertEqual((ret, value), (0, expect_value))
            else:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
            for key in self.extra_keys:
                self.assertEqual(self.search(self.uri, ts, key)[0], wiredtiger.WT_NOTFOUND)

    def test_stepup_remove(self):
        self.extra_keys = getattr(self, 'extra_keys', [])
        self.session.create(self.uri, 'key_format=S,value_format=u')
        # A second table whose remove deletes a live key: it must not produce a warning.
        self.session.create(self.live_uri, 'key_format=S,value_format=u')
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
        c = self.session.open_cursor(self.live_uri)
        self.session.begin_transaction()
        c['k'] = b'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # Follower: write each remove into the ingest tables.
        self.conn.reconfigure('disaggregated=(role="follower")')
        c = self.session.open_cursor(self.ingest_uri)
        for ts in self.remove_ts:
            self.session.begin_transaction()
            c['k'] = b'\x14\x14'
            for key in self.extra_keys:
                c[key] = b'\x14\x14'
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        if self.insert_ts:
            self.session.begin_transaction()
            c['k'] = b'w'
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.insert_ts))
        c.close()
        c = self.session.open_cursor(self.live_ingest_uri)
        self.session.begin_transaction()
        c['k'] = b'\x14\x14'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        c.close()

        # Let checkpoints include the removes while readers can still read before them.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        orphans = 0 if self.stable_state == 'present' else 1 + len(self.extra_keys)
        warning = f'{self.uri}: step-up drain skipped {orphans} remove(s) of keys with no ' \
            'value in the stable table'
        before = self.orphan_stat()
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="leader")')
        output = self.readStdout()
        self.cleanStdout()

        # One warning per table with orphans, naming the table, and no other warnings.
        self.assertEqual(output.count(self.skip_msg), 1 if orphans else 0)
        self.assertEqual(output.count(warning), 1 if orphans else 0)
        self.assertEqual(self.orphan_stat() - before, orphans)

        # Reads at every timestamp: after the step-up, after the stable pages are written, and
        # after a restart.
        self.check_reads()
        self.session.checkpoint()
        self.check_reads()
        self.reopen_conn()
        self.check_reads()

        c = self.session.open_cursor(self.uri)
        self.assertEqual(c['other'], b'v')
        c.close()
        self.assertEqual(self.search(self.live_uri)[0], wiredtiger.WT_NOTFOUND)
