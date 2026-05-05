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

import wttest
from wiredtiger import stat

# test_truncate30.py
# Write conflict with uncommitted insert

class test_truncate30(wttest.WiredTigerTestCase):
    uri = 'file:test_truncate30'
    conn_config = 'statistics=(all)'
    session_create_config = 'key_format=Q,value_format=S'

    def test_truncate30(self):
        self.session.create(self.uri, self.session_create_config)
        c = self.session.open_cursor(self.uri, None)
        for x in range(0, 1000):
            c.set_key(x)
            c.set_value(str(x))
            c.insert()

        # Have a long-running transaction. Don't commit or roll this back.
        s2 = self.conn.open_session()
        s2.begin_transaction()
        c1 = s2.open_cursor(self.uri, None)
        c1.set_key(100)
        c1.set_value('abcdef')
        c1.insert()

        c2 = self.session.open_cursor(self.uri, None)
        c3 = self.session.open_cursor(self.uri, None)

        c2.set_key(99)
        c3.set_key(101)
        c2.search()
        c3.search()
        self.session.begin_transaction()
        self.session.truncate(None, c2, c3, None)
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
