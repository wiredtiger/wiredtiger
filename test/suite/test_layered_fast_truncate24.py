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

from contextlib import closing
import time
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_fast_truncate import LayeredFastTruncateConfigMixin
from wiredtiger import stat
from wtscenario import make_scenarios


@disagg_test_class
class test_layered_fast_truncate24(LayeredFastTruncateConfigMixin, wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),disaggregated=(role="leader")'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('layered', dict(uri='layered:test_layered_fast_truncate24')),
        ('table', dict(uri='table:test_layered_fast_truncate24')),
    ])

    def setup_tables(self, ingest=False):
        self.other_uri = self.uri + '_other'
        self.session.create(self.other_uri, self.session_create_config())
        self.setup_leader(keys=range(1, 101))
        self.setup_follower(keys=range(1, 101) if ingest else None)

    def metric(self, name, uri=None):
        with closing(self.session.open_cursor('statistics:' + (uri or ''), None,
                'statistics=(all)')) as cursor:
            key = getattr(stat.dsrc if uri else stat.conn, 'layered_truncate_' + name)
            return cursor[key][2]

    def truncate_on(self, session, start, stop):
        with closing(session.open_cursor(self.uri)) as lo, \
                closing(session.open_cursor(self.uri)) as hi:
            lo.set_key(start)
            hi.set_key(stop)
            session.truncate(None, lo, hi, None)

    def test_list_lifecycle(self):
        # FIXME-WT-18854: Re-enable when layered table statistics report all updates.
        self.skipTest('Layered table statistics reporting is incomplete.')
        self.setup_tables()
        self.ignoreStdoutPattern('Picking up the same checkpoint')
        self.truncate(20, 40, commit_timestamp=20)
        self.assertEqual(self.metric('list_entries_inserted', self.other_uri), 0)
        with closing(self.session.open_cursor(self.other_uri)) as cursor:
            with self.transaction(commit_timestamp=30):
                cursor[1] = 'v'
        with closing(self.conn.open_session()) as other:
            other.begin_transaction()
            other.truncate(self.other_uri, None, None, None)
            other.commit_transaction('commit_timestamp=' + self.timestamp_str(40))
            self.assertEqual(self.metric('list_entries_inserted'), 2)
            for uri in (self.uri, self.other_uri):
                self.assertEqual(self.metric('list_entries_inserted', uri), 1)
            other.begin_transaction()
            self.truncate_on(other, 60, 80)
            for uri in (None, self.uri):
                self.assertEqual(self.metric('list_entries_inserted', uri), 3 if uri is None else 2)
            other.rollback_transaction()
            for uri in (None, self.uri):
                self.assertEqual(self.metric('list_rollback_entries_removed', uri), 1)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.reconfigure('disaggregated=(role="leader")')
        for uri in (None, self.uri, self.other_uri):
            self.assertEqual(self.metric('list_clear_entries_removed', uri), 2 if uri is None else 1)

    def test_garbage_collection(self):
        self.setup_leader(keys=range(1, 101))
        self.leader_checkpoint(10)
        follower, session = self.open_follower(self.session_create_config())
        with closing(follower):
            with closing(session):
                for start, stop, timestamp in ((20, 40, 20), (60, 70, 30)):
                    session.begin_transaction()
                    self.truncate_on(session, start, stop)
                    session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))

                metrics = [stat.conn.layered_truncate_list_gc_runs,
                    stat.conn.layered_truncate_list_gc_entries_examined,
                    stat.conn.layered_truncate_list_gc_entries_removed]
                before = [self.get_stat(metric, conn=follower) for metric in metrics]
                self.assertEqual(before[2], 0)
                self.truncate(20, 40, commit_timestamp=20)
                self.leader_checkpoint(20)
                self.disagg_advance_checkpoint_and_wait(follower)

                deadline = time.time() + 60
                while self.get_stat(metrics[2], conn=follower) == before[2]:
                    self.assertLess(time.time(), deadline, 'truncate list garbage collection timed out')
                    session.begin_transaction()
                    try:
                        self.truncate_on(session, 80, 80)
                    finally:
                        session.rollback_transaction()

                after = [self.get_stat(metric, conn=follower) for metric in metrics]
                self.assertGreater(after[0], before[0])
                self.assertGreater(after[1], before[1])
                self.assertEqual(after[2] - before[2], 1)

    def test_search_hits_and_misses(self):
        self.setup_tables()
        before_hits = self.metric('list_search_hits')
        before_misses = self.metric('list_search_misses')
        before_walked = self.metric('list_search_entries_walked')
        self.assertTrue(self.key_exists(80))
        self.assertEqual(self.metric('list_search_hits'), before_hits)
        self.assertEqual(self.metric('list_search_misses') - before_misses, 1)
        self.assertEqual(self.metric('list_search_entries_walked'), before_walked)
        self.truncate(20, 40, commit_timestamp=20)
        before_hit = self.metric('list_search_hits')
        before_miss = self.metric('list_search_misses')
        before_walked = self.metric('list_search_entries_walked')
        before_table_hit = self.metric('list_search_hits', self.uri)
        before_table_miss = self.metric('list_search_misses', self.uri)
        self.assertFalse(self.key_exists(25))
        self.assertEqual(self.metric('list_search_hits') - before_hit, 1)
        self.assertEqual(self.metric('list_search_misses') - before_miss, 0)
        self.assertEqual(self.metric('list_search_entries_walked') - before_walked, 1)
        self.assertTrue(self.key_exists(80))
        self.assertEqual(self.metric('list_search_hits') - before_hit, 1)
        self.assertEqual(self.metric('list_search_misses') - before_miss, 1)
        self.assertEqual(self.metric('list_search_entries_walked') - before_walked, 2)
        self.assertEqual(self.metric('list_search_hits', self.uri) - before_table_hit, 1)
        self.assertEqual(self.metric('list_search_hits', self.other_uri), 0)
        self.assertEqual(self.metric('list_search_misses', self.uri) - before_table_miss, 1)
        self.assertEqual(self.metric('list_search_misses', self.other_uri), 0)

        before_hit = self.metric('list_search_hits')
        before_miss = self.metric('list_search_misses')
        with closing(self.session.open_cursor(self.uri, None, 'overwrite=false')) as cursor:
            with self.transaction(rollback=True):
                cursor.set_key(80)
                cursor.set_value('duplicate')
                self.assertRaisesException(wiredtiger.WiredTigerError,
                    cursor.insert, '/WT_DUPLICATE_KEY/')
        self.assertEqual(self.metric('list_search_hits'), before_hit)
        self.assertGreater(self.metric('list_search_misses'), before_miss)

        self.truncate(60, 70, commit_timestamp=30)
        for key, exists, walked in ((25, False, 1), (65, False, 2), (80, True, 2)):
            before_walked = self.metric('list_search_entries_walked')
            self.assertEqual(self.key_exists(key), exists)
            self.assertEqual(self.metric('list_search_entries_walked') - before_walked, walked)

    def test_ingest_work(self):
        # FIXME-WT-18854: Re-enable when layered table statistics report all updates.
        self.skipTest('Layered table statistics reporting is incomplete.')
        self.setup_tables(ingest=True)
        self.truncate(20, 40, commit_timestamp=20)
        for uri in (None, self.uri):
            self.assertEqual(self.metric('ingest_tombstones_written', uri), 21)
        # Revisit the deleted keys and add one live key at each boundary.
        self.truncate(19, 41, commit_timestamp=30)
        for uri in (None, self.uri):
            self.assertEqual(self.metric('ingest_tombstones_written', uri), 23)
        self.assertEqual(self.metric('ingest_tombstones_written', self.other_uri), 0)

    def test_write_conflicts(self):
        # FIXME-WT-18854: Re-enable when layered table statistics report all updates.
        self.skipTest('Layered table statistics reporting is incomplete.')
        self.setup_tables()
        self.session.begin_transaction()
        self.truncate_on(self.session, 20, 40)
        with closing(self.conn.open_session()) as other, \
                closing(other.open_cursor(self.uri)) as cursor:
            cursor.set_key(25)
            cursor.set_value('conflict')
            for count, operation in enumerate(
                    (lambda: self.truncate_on(other, 30, 50), cursor.insert), 1):
                other.begin_transaction()
                before_hit = self.metric('list_search_hits')
                before_miss = self.metric('list_search_misses')
                before_walked = self.metric('list_search_entries_walked')
                self.assertRaisesException(wiredtiger.WiredTigerError, operation, '/conflict/')
                self.assertEqual(self.metric('list_search_hits') - before_hit, 1)
                self.assertEqual(self.metric('list_search_misses'), before_miss)
                self.assertEqual(self.metric('list_search_entries_walked') - before_walked, 1)
                other.rollback_transaction()
                for uri in (None, self.uri):
                    self.assertEqual(self.metric('list_write_conflicts', uri), count)
        self.session.rollback_transaction()
        for uri in (None, self.uri):
            self.assertGreater(self.metric('list_search_hits', uri), 0)
