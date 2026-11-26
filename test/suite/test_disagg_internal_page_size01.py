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
from helper_disagg import disagg_test_class
from wiredtiger import stat

# A disaggregated internal page built from very large keys must be split rather than written as a
# single block larger than the page log can carry.
@disagg_test_class
class test_disagg_internal_page_size01(wttest.WiredTigerTestCase):
    conn_config = 'disaggregated=(role="leader",lose_all_my_data=true),cache_size=2GB'
    uri = 'table:large_internal_keys'
    table_config = 'key_format=S,value_format=S,block_manager=disagg,type=file,' \
        'internal_page_max=4KB,leaf_page_max=4KB'

    # With 8MB keys, forty children would form a single internal page of well over 32MB.
    nkeys = 40
    key_size = 8 * 1024 * 1024
    max_write_size = 32 * 1024 * 1024

    def make_key(self, i):
        # The keys differ only in their tail, so the key promoted to the parent is nearly as large
        # as the key itself.
        return 'k' * (self.key_size - 16) + f'{i:016d}'

    def test_large_internal_keys(self):
        self.session.create(self.uri, self.table_config)
        with wttest.open_cursor(self.session, self.uri) as cursor:
            for i in range(self.nkeys):
                with self.transaction(commit_timestamp=i + 1):
                    cursor[self.make_key(i)] = 'v'
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.nkeys))

        self.session.checkpoint()

        # Reopen without wiping the page store, so every page is read back from its checkpoint.
        self.conn_config = self.conn_config.replace(',lose_all_my_data=true', '')
        self.reopen_conn()

        # Each internal page stays under the maximum write size, so the keys need several of them.
        with wttest.open_cursor(self.session, self.uri, config='debug=(size_stats)') as cursor:
            while cursor.next() == 0:
                pass
        with wttest.open_cursor(self.session, 'statistics:' + self.uri,
                                config='statistics=(fast)') as cursor:
            internal_pages = cursor[stat.dsrc.btree_size_internal_pages][2]
        min_pages = self.nkeys * self.key_size // self.max_write_size
        self.assertGreaterEqual(internal_pages, min_pages)

        with wttest.open_cursor(self.session, self.uri) as cursor:
            for i in range(self.nkeys):
                cursor.set_key(self.make_key(i))
                self.assertEqual(cursor.search(), 0)
                self.assertEqual(cursor.get_value(), 'v')

    def test_page_max_over_limit(self):
        for option in ('internal_page_max', 'leaf_page_max'):
            uri = 'table:over_limit_' + option
            config = 'key_format=S,value_format=S,block_manager=disagg,type=file,' + \
                option + '=64MB'
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.create(uri, config), '/must not exceed/')

    def test_page_max_at_limit(self):
        self.session.create('table:at_limit',
            'key_format=S,value_format=S,block_manager=disagg,type=file,' +
            'internal_page_max=32MB,leaf_page_max=32MB')
        with wttest.open_cursor(self.session, 'table:at_limit') as cursor:
            with self.transaction(commit_timestamp=1):
                cursor['key'] = 'value'

    def test_oversized_item_rejected(self):
        self.session.create(self.uri, self.table_config)
        mb = 1024 * 1024
        limit = 32 * mb
        too_large = '/exceeds the maximum size/'
        with wttest.open_cursor(self.session, self.uri) as cursor:
            # A value at the limit, one that leaves no room for the headers, and a key and value
            # that are each under the limit but over it together.
            for key, value in (('key', 'x' * limit),
                               ('key', 'x' * (limit - 100)),
                               ('k' * (16 * mb), 'x' * (17 * mb))):
                with self.transaction(commit_timestamp=1):
                    cursor.set_key(key)
                    cursor.set_value(value)
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: cursor.insert(), too_large)
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: cursor.update(), too_large)

            # The largest MongoDB document is accepted.
            with self.transaction(commit_timestamp=2):
                cursor['max'] = 'x' * (16 * mb)

            # A modify that grows the value past the limit.
            with self.transaction(commit_timestamp=3):
                cursor['key'] = 'x' * (20 * mb)
            with self.transaction(commit_timestamp=4):
                cursor.set_key('key')
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: cursor.modify([wiredtiger.Modify('y' * (14 * mb), 20 * mb, 0)]),
                    too_large)
