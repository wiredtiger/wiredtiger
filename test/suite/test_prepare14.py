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

import wttest
from wiredtiger import WT_NOTFOUND
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_prepare14.py
# Test that the transaction visibility of an on-disk update
# that has both the start and the stop time points from the
# same uncommitted prepared transaction.
class test_prepare14(wttest.WiredTigerTestCase):
    session_config = 'isolation=snapshot'

    in_memory_values = [
        ('no_inmem', dict(in_memory=False)),
        ('inmem', dict(in_memory=True))
    ]

    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    scenarios = make_scenarios(in_memory_values, key_format_values)

    def conn_config(self):
        config = 'cache_size=50MB'
        if self.in_memory:
            config += ',in_memory=true'
        else:
            config += ',in_memory=false'
        return config

    def test_prepare14(self):
        # Prepare transactions for column store table is not yet supported.
        if self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        uri = "table:prepare14"
        create_config = 'allocation_size=512,key_format=S,value_format=S'
        self.session.create(uri, create_config)

        # Pin oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        value = 'a'

        # Perform several updates and removes.
        s = self.conn.open_session()
        cursor = s.open_cursor(uri)
        s.begin_transaction()
        cursor[str(0)] = value
        cursor.set_key(str(0))
        cursor.remove()
        cursor.close()
        s.prepare_transaction('prepare_timestamp=' + timestamp_str(20))

        # Configure debug behavior on a cursor to evict the page positioned on when the reset API is used.
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search for the key so we position our cursor on the page that we want to evict.
        self.session.begin_transaction("ignore_prepare = true")
        evict_cursor.set_key(str(0))
        self.assertEquals(evict_cursor.search(), WT_NOTFOUND)
        evict_cursor.reset()
        evict_cursor.close()
        self.session.commit_transaction()

        self.session.begin_transaction("ignore_prepare = true")
        cursor2 = self.session.open_cursor(uri)
        cursor2.set_key(str(0))
        self.assertEquals(cursor2.search(), WT_NOTFOUND)
        self.session.commit_transaction()
