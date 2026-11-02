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

import wiredtiger, wttest
from helper import WiredTigerCursor
from helper_disagg import disagg_test_class, DisaggSchemaEpochMixin, gen_disagg_storages
from metadata_helper import get_table_id
from wiredtiger import stat
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema37(DisaggSchemaEpochMixin, wttest.WiredTigerTestCase):
    conn_config = 'precise_checkpoint=true,disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = 'precise_checkpoint=true,statistics=(all),disaggregated=(role="follower")'
    layered_uri = 'layered:test_layered_schema37'
    ingest_uri = 'file:test_layered_schema37.wt_ingest'

    creation_formats = [
        ('layered', dict(uri='layered:test_layered_schema37', extra_config='')),
        ('table', dict(uri='table:test_layered_schema37',
            extra_config=',type=layered,block_manager=disagg')),
    ]
    data_formats = [
        ('string', dict(key_format='S', value_format='S', key='key', value='value')),
        ('integer', dict(key_format='i', value_format='I', key=1, value=1)),
    ]
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True),
        creation_formats, data_formats)

    def table_config(self):
        return f'key_format={self.key_format},value_format={self.value_format}' + self.extra_config

    def create_table(self, uri, timestamp=1):
        self.session.create(uri, self.table_config())
        with self.transaction(commit_timestamp=timestamp):
            with WiredTigerCursor(self.session, uri) as cursor:
                cursor[self.key] = self.value

    def start_follower(self):
        self.create_table(self.uri)
        self.leader_checkpoint(1)
        self.conn_follow, self.session_follow = self.open_follower()

    def follower_metadata(self, uri):
        with WiredTigerCursor(self.session_follow, 'metadata:') as cursor:
            return cursor[uri]

    def lazy_ingest_creation_count(self):
        return self.get_stat(stat.conn.disagg_ingest_create_on_open, session=self.session_follow)

    def assert_missing_file(self, uri):
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session_follow.open_cursor(uri), '/No such file or directory/')
        with WiredTigerCursor(self.session_follow, 'metadata:') as cursor:
            cursor.set_key(uri)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

    def test_startup_defers_ingest_creation(self):
        self.start_follower()
        self.assertFalse(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))

    def test_direct_ingest_open_creates_metadata(self):
        self.start_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        self.assertTrue(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))

    def test_layered_open_creates_ingest_and_reads_checkpoint(self):
        self.start_follower()
        with WiredTigerCursor(self.session_follow, self.uri) as cursor:
            self.assertEqual(cursor[self.key], self.value)
        self.assertTrue(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))

    def test_untouched_table_remains_deferred(self):
        self.create_table(self.uri + '_other')
        self.start_follower()
        self.session_follow.open_cursor(self.uri).close()
        self.assertFalse(self.uri_in_local_metadata(self.conn_follow, self.layered_uri + '_other'))

    def test_created_ingest_preserves_configuration(self):
        self.start_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        config = self.follower_metadata(self.ingest_uri)
        for setting in (f'key_format="{self.key_format}"', f'value_format="{self.value_format}"',
            'in_memory=true', 'log=(enabled=false)'):
            self.assertIn(setting, config)

    def test_ingest_metadata_is_local(self):
        self.start_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        with WiredTigerCursor(self.session_follow, 'file:WiredTigerShared.wt_stable') as cursor:
            self.assertNotIn(self.ingest_uri, dict(cursor))

    def test_repeated_open_reuses_ingest(self):
        self.start_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        ingest_id = get_table_id(self.session_follow, self.ingest_uri)
        creations = self.lazy_ingest_creation_count()
        self.session_follow.open_cursor(self.uri).close()
        self.assertEqual(get_table_id(self.session_follow, self.ingest_uri), ingest_id)
        self.assertEqual(self.lazy_ingest_creation_count(), creations)

    def test_reopen_reuses_ingest_metadata(self):
        self.start_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        ingest_id = get_table_id(self.session_follow, self.ingest_uri)
        self.session_follow.close()
        self.conn_follow.close()
        self.conn_follow, self.session_follow = self.open_follower()
        self.session_follow.open_cursor(self.ingest_uri).close()
        self.assertEqual(get_table_id(self.session_follow, self.ingest_uri), ingest_id)
        self.assertEqual(self.lazy_ingest_creation_count(), 0)

    def test_missing_parent_does_not_create_ingest(self):
        self.start_follower()
        self.assert_missing_file('file:missing.wt_ingest')

    def test_empty_name_does_not_create_ingest(self):
        self.start_follower()
        self.assert_missing_file('file:.wt_ingest')

    def test_mismatched_parent_does_not_create_ingest(self):
        self.start_follower()
        config = self.follower_metadata(self.layered_uri) + ',ingest="file:other.wt_ingest"'
        self.inject_stable_entry(self.conn_follow, self.layered_uri, config)
        self.assert_missing_file(self.ingest_uri)

    def test_dropped_parent_does_not_create_ingest(self):
        self.start_follower()
        self.session_follow.drop(self.uri)
        self.assert_missing_file(self.ingest_uri)

    def test_transaction_rollback_preserves_created_ingest(self):
        self.start_follower()
        with self.transaction(session=self.session_follow, rollback=True):
            with WiredTigerCursor(self.session_follow, self.uri) as cursor:
                self.assertEqual(cursor[self.key], self.value)
        self.assertTrue(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))

    def test_first_open_after_step_up(self):
        self.start_follower()
        self.step_up(self.conn_follow)
        self.assertFalse(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))
        with WiredTigerCursor(self.session_follow, self.uri) as cursor:
            self.assertEqual(cursor[self.key], self.value)
        self.assertTrue(self.uri_in_local_metadata(self.conn_follow, self.layered_uri))
        self.step_down(self.conn_follow)

    def test_steady_state_pickup_creates_ingest_eagerly(self):
        self.start_follower()
        self.create_table(self.uri + '_new', timestamp=2)
        self.leader_checkpoint(2)
        self.disagg_advance_checkpoint_and_wait(self.conn_follow)
        self.assertTrue(self.uri_in_local_metadata(self.conn_follow, self.layered_uri + '_new'))
        self.assertEqual(self.lazy_ingest_creation_count(), 0)
