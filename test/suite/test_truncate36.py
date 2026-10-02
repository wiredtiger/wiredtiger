#!/usr/bin/env python
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


class test_truncate36(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=200MB,statistics=(fast)'
    uri = 'table:test_truncate36'
    other_uri = 'table:test_truncate36_other'
    config = 'key_format=i,value_format=S,allocation_size=512,leaf_page_max=4096'

    def get_stat(self, uri, name):
        cursor = self.session.open_cursor('statistics:' + uri)
        key = getattr(wiredtiger.stat.conn if not uri else wiredtiger.stat.dsrc, name)
        value = cursor[key][2]
        cursor.close()
        return value

    def truncate(self, uri, first, last):
        start = self.session.open_cursor(uri)
        stop = self.session.open_cursor(uri)
        start.set_key(first)
        stop.set_key(last)
        self.session.begin_transaction()
        self.session.truncate(None, start, stop, None)
        start.close()
        stop.close()
        self.session.commit_transaction()

    def test_range_truncate_stats(self):
        for uri in (self.uri, self.other_uri):
            self.session.create(uri, self.config)
            cursor = self.session.open_cursor(uri)
            for key in range(1, 10001):
                cursor[key] = 'a' * 100
            if uri == self.uri:
                cursor[5000] = 'a' * 12000
            cursor.close()
        self.session.checkpoint()
        self.reopen_conn()

        names = (
            'truncate_boundary_leaf_pages_read',
            'truncate_fast_delete_fallback_in_memory',
            'truncate_fast_delete_fallback_pages',
            'truncate_fast_deleted_leaf_pages',
            'truncate_internal_bytes_dirtied',
            'truncate_internal_pages_dirtied',
            'truncate_leaf_bytes_dirtied',
            'truncate_leaf_pages_dirtied',
            'truncate_slow_path_leaf_pages',
            'truncate_slow_path_update_bytes',
        )
        before_conn = {name: self.get_stat('', name) for name in names}
        before_first = {name: self.get_stat(self.uri, name) for name in names}
        before_other = {name: self.get_stat(self.other_uri, name) for name in names}

        self.truncate(self.uri, 100, 9900)

        first = {name: self.get_stat(self.uri, name) - before_first[name] for name in names}
        for name in names:
            self.assertEqual(self.get_stat('', name) - before_conn[name], first[name])
            self.assertEqual(self.get_stat(self.other_uri, name), before_other[name])

        self.assertGreater(first['truncate_fast_deleted_leaf_pages'], 0)
        self.assertGreater(first['truncate_slow_path_leaf_pages'], 0)
        self.assertGreater(first['truncate_leaf_pages_dirtied'], 0)
        self.assertGreater(first['truncate_leaf_bytes_dirtied'], 0)
        self.assertGreater(first['truncate_internal_pages_dirtied'], 0)
        self.assertGreater(first['truncate_internal_bytes_dirtied'], 0)
        self.assertGreater(first['truncate_slow_path_update_bytes'], 0)
        self.assertGreater(first['truncate_boundary_leaf_pages_read'], 0)
        self.assertGreater(
            first['truncate_fast_delete_fallback_pages'],
            first['truncate_fast_delete_fallback_in_memory'])
        self.assertGreaterEqual(
            self.get_stat(self.uri, 'rec_page_delete_fast'),
            first['truncate_fast_deleted_leaf_pages'])

        cursor = self.session.open_cursor(self.other_uri)
        cursor[5000] = 'b' * 100
        cursor.close()
        for name in names:
            self.assertEqual(self.get_stat(self.other_uri, name), before_other[name])
        self.truncate(self.other_uri, 100, 9900)
        self.assertGreater(
            self.get_stat(self.other_uri, 'truncate_fast_delete_fallback_in_memory') -
            before_other['truncate_fast_delete_fallback_in_memory'], 0)
        for name in names:
            self.assertEqual(self.get_stat(self.uri, name) - before_first[name], first[name])
            self.assertEqual(
                self.get_stat('', name) - before_conn[name],
                first[name] + self.get_stat(self.other_uri, name) - before_other[name])
