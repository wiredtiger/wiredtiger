#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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

import time, wiredtiger, wttest, unittest
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_hs19.py
# Test that we don't increment statistics relating to mixed mode timestamp operations when not using
# timestamps.
class test_hs19(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB,eviction=(threads_max=1),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_base_scenario(self):
        uri = 'table:test_base_scenario'
        self.session.create(uri, 'key_format=S,value_format=S')
        session2 = self.setUpSessionOpen(self.conn)
        cursor = self.session.open_cursor(uri)
        value3 = 'c' * 500
        value4 = 'd' * 500

        # Start a long running transaction.
        session2.begin_transaction()

        for i in range(1, 2000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        # Insert a new update over the old ones. Eviction will begin moving content to the history
        # store.
        for i in range(1, 2000):
            self.session.begin_transaction()
            cursor[str(i)] = value4
            self.session.commit_transaction()

        # Validate that we haven't incorrectly incremented that statistic.
        c = self.session.open_cursor( 'statistics:')
        self.assertEqual(c[stat.conn.cache_hs_key_truncate_mix_ts][2], 0)
        c.close()
