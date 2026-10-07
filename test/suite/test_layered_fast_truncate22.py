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
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_fast_truncate import LayeredFastTruncateConfigMixin
from wiredtiger import stat
from wtscenario import make_scenarios


@disagg_test_class
class test_layered_fast_truncate22(LayeredFastTruncateConfigMixin, wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),disaggregated=(role="leader")'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('layered', dict(uri='layered:test_layered_fast_truncate22')),
        ('table', dict(uri='table:test_layered_fast_truncate22')),
    ])

    def setup_tables(self, ingest=False):
        self.other_uri = self.uri + '_other'
        self.session.create(self.other_uri, self.session_create_config())
        self.setup_leader(keys=range(1, 101))
        self.setup_follower(keys=range(1, 101) if ingest else None)

    def metric(self, name, uri=None, clear=False):
        with closing(self.session.open_cursor('statistics:' + (uri or ''), None,
                'statistics=(all' + (',clear' if clear else '') + ')')) as cursor:
            key = getattr(stat.dsrc if uri else stat.conn, 'layered_truncate_' + name)
            return cursor[key][2]

    def truncate_on(self, session, start, stop):
        with closing(session.open_cursor(self.uri)) as lo, \
                closing(session.open_cursor(self.uri)) as hi:
            lo.set_key(start)
            hi.set_key(stop)
            session.truncate(None, lo, hi, None)

    def test_list_lifecycle(self):
        self.setup_tables()
        self.session.begin_transaction()
        self.truncate_on(self.session, 20, 40)
        for uri in (None, self.uri):
            self.assertEqual(self.metric('list_entries_inserted', uri), 1)
            self.assertEqual(self.metric('list_entries_current', uri), 1)
        self.assertEqual(self.metric('list_entries_inserted', self.other_uri), 0)
        self.session.rollback_transaction()
        for uri in (None, self.uri):
            self.assertEqual(self.metric('list_entries_current', uri), 0)
            self.assertEqual(self.metric('list_rollback_entries_removed', uri), 1)

    def test_live_gauges_after_enabling_statistics(self):
        self.setup_tables()
        self.ignoreStdoutPattern('Picking up the same checkpoint')
        self.conn.reconfigure('statistics=(none)')
        self.truncate(20, 40, commit_timestamp=20)
        self.conn.reconfigure('statistics=(all)')
        for uri in (None, self.uri):
            self.assertEqual(self.metric('list_entries_current', uri), 1)

    def test_clear_preserves_live_gauges(self):
        self.setup_tables()
        self.truncate(20, 40, commit_timestamp=20)
        self.session.begin_transaction()
        self.truncate_on(self.session, 60, 80)
        self.session.rollback_transaction()
        for uri in (None, self.uri):
            self.assertEqual(self.metric('list_entries_current', uri, clear=True), 1)
            self.assertEqual(self.metric('list_entries_current', uri), 1)
            self.assertEqual(self.metric('list_entries_inserted', uri), 0)

    def test_connection_combines_tables(self):
        self.setup_tables()
        self.truncate(20, 40, commit_timestamp=20)
        with closing(self.session.open_cursor(self.other_uri)) as cursor:
            with self.transaction(commit_timestamp=30):
                cursor[1] = 'v'
        with self.transaction(commit_timestamp=40):
            self.session.truncate(self.other_uri, None, None, None)
        self.assertEqual(self.metric('list_entries_current'), 2)
        self.assertEqual(self.metric('list_entries_current', self.uri), 1)
        self.assertEqual(self.metric('list_entries_current', self.other_uri), 1)

    def test_search_hits_and_misses(self):
        self.setup_tables()
        self.truncate(20, 40, commit_timestamp=20)
        before_hit = self.metric('list_search_hits')
        before_miss = self.metric('list_search_misses')
        before_table_hit = self.metric('list_search_hits', self.uri)
        before_table_miss = self.metric('list_search_misses', self.uri)
        self.assertFalse(self.key_exists(25))
        self.assertEqual(self.metric('list_search_hits') - before_hit, 1)
        self.assertEqual(self.metric('list_search_misses') - before_miss, 0)
        self.assertTrue(self.key_exists(80))
        self.assertEqual(self.metric('list_search_hits') - before_hit, 1)
        self.assertEqual(self.metric('list_search_misses') - before_miss, 1)
        self.assertEqual(self.metric('list_search_hits', self.uri) - before_table_hit, 1)
        self.assertEqual(self.metric('list_search_hits', self.other_uri), 0)
        self.assertEqual(self.metric('list_search_misses', self.uri) - before_table_miss, 1)
        self.assertEqual(self.metric('list_search_misses', self.other_uri), 0)

    def test_empty_list_search_miss(self):
        self.setup_tables()
        before_hits = self.metric('list_search_hits')
        before_misses = self.metric('list_search_misses')
        self.assertTrue(self.key_exists(80))
        self.assertEqual(self.metric('list_search_hits'), before_hits)
        self.assertEqual(self.metric('list_search_misses') - before_misses, 1)

    def test_write_probe_counted_as_search(self):
        self.setup_tables()
        self.truncate(20, 40, commit_timestamp=20)
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

    def test_ingest_work(self):
        self.setup_tables(ingest=True)
        self.truncate(20, 40, commit_timestamp=20)
        for uri in (None, self.uri):
            self.assertEqual(self.metric('ingest_keys_walked', uri), 21)
            self.assertEqual(self.metric('ingest_tombstones_written', uri), 21)
        # Revisit the deleted keys and add one live key at each boundary.
        self.truncate(19, 41, commit_timestamp=30)
        for uri in (None, self.uri):
            self.assertEqual(self.metric('ingest_keys_walked', uri), 44)
            self.assertEqual(self.metric('ingest_tombstones_written', uri), 23)
        self.assertEqual(self.metric('ingest_keys_walked', self.other_uri), 0)

    def test_write_conflicts(self):
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
                self.assertRaisesException(wiredtiger.WiredTigerError, operation, '/conflict/')
                other.rollback_transaction()
                for uri in (None, self.uri):
                    self.assertEqual(self.metric('list_write_conflicts', uri), count)
        self.session.rollback_transaction()
        for uri in (None, self.uri):
            self.assertGreater(self.metric('list_search_hits', uri), 0)

    def test_stepup_clears_list(self):
        self.setup_tables()
        self.ignoreStdoutPattern('Picking up the same checkpoint')
        self.truncate(20, 40, commit_timestamp=20)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        self.conn.reconfigure('disaggregated=(role="leader")')
        for uri in (None, self.uri):
            self.assertEqual(self.metric('list_entries_current', uri), 0)
            self.assertEqual(self.metric('list_clear_entries_removed', uri), 1)
