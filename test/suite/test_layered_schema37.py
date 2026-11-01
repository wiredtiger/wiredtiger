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

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wttest import open_cursor

# Ingest constituents remain absent from metadata throughout a layered table's lifecycle.
@disagg_test_class
class test_layered_schema37(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = 'cache_cursors=false,statistics=(all),disaggregated=(role=leader)'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    table_types = [
        ('layered', dict(prefix='layered:', extra_config='')),
        ('table', dict(prefix='table:', extra_config=',type=layered')),
    ]
    formats = [
        ('string', dict(key_format='S', key='key')),
        ('integer', dict(key_format='i', key=1)),
    ]
    scenarios = make_scenarios(disagg_storages, table_types, formats)

    @property
    def uri(self):
        return self.prefix + self.test_name

    @property
    def ingest_uri(self):
        return f'file:{self.test_name}.wt_ingest'

    def create_table(self):
        self.session.create(self.uri, f'key_format={self.key_format},value_format=S,' +
            'allocation_size=512,internal_page_max=4096,leaf_page_max=8192' + self.extra_config)

    def assert_no_ingest_metadata(self, session):
        with open_cursor(session, 'metadata:') as cursor:
            cursor.set_key(self.ingest_uri)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
            cursor.reset()
            self.assertNotIn(self.ingest_uri, dict(cursor))

    def write_row(self):
        with open_cursor(self.session, self.uri) as cursor:
            self.session.begin_transaction()
            cursor[self.key] = 'value'
            self.session.commit_transaction('commit_timestamp=a')
            self.assertEqual(cursor[self.key], 'value')

    def test_create_and_access(self):
        self.create_table()
        self.assert_no_ingest_metadata(self.session)
        self.write_row()
        with open_cursor(self.session, self.ingest_uri) as cursor:
            self.assertEqual(cursor.key_format, self.key_format)
            self.assertEqual(cursor.value_format, 'S')
            self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        for config in ('statistics=(all)', 'statistics=(size)'):
            with open_cursor(self.session, 'statistics:' + self.ingest_uri, config=config) as cursor:
                self.assertEqual(cursor[wiredtiger.stat.dsrc.block_size][2], 0)
        self.assert_no_ingest_metadata(self.session)

    def test_direct_ingest_and_step_up(self):
        self.conn.reconfigure('disaggregated=(role=follower)')
        self.create_table()
        self.assert_no_ingest_metadata(self.session)
        with open_cursor(self.session, self.ingest_uri) as cursor:
            self.session.begin_transaction()
            cursor[self.key] = 'value'
            self.session.commit_transaction('commit_timestamp=a')
        with open_cursor(self.session, self.uri) as cursor:
            self.assertEqual(cursor[self.key], 'value')
        self.conn.reconfigure('disaggregated=(role=leader)')
        with open_cursor(self.session, self.uri) as cursor:
            self.assertEqual(cursor[self.key], 'value')
        self.assert_no_ingest_metadata(self.session)

    def test_pickup_and_reopen(self):
        self.create_table()
        self.write_row()
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=a')
        self.session.checkpoint()
        config = self.extensionsConfig() + ',create,cache_cursors=false,disaggregated=(role=follower)'
        for _ in range(2):
            conn = self.wiredtiger_open('follower', config)
            session = conn.open_session()
            self.disagg_advance_checkpoint(conn)
            self.assert_no_ingest_metadata(session)
            with open_cursor(session, self.uri) as cursor:
                self.assertEqual(cursor[self.key], 'value')
            self.assert_no_ingest_metadata(session)
            session.close()
            conn.close()

    def test_drop_and_recreate(self):
        self.create_table()
        self.assert_no_ingest_metadata(self.session)
        self.session.drop(self.uri)
        self.create_table()
        self.write_row()
        self.conn.set_timestamp('oldest_timestamp=a,stable_timestamp=a')
        self.session.checkpoint()
        self.session.drop(self.uri)
        self.assert_no_ingest_metadata(self.session)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.ingest_uri), '/No such file or directory/')
        self.create_table()
        with open_cursor(self.session, self.uri) as cursor:
            self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        self.assert_no_ingest_metadata(self.session)
