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
#
# test_timestamp27.py
#   Timestamps: assert commit settings
#

import wiredtiger, wttest

# Test query-timestamp returns 0 if the timestamp is not set.
class test_timestamp27_query_notset(wttest.WiredTigerTestCase):
    def test_conn_query_notset(self):
        self.assertEquals(self.conn.query_timestamp('get=all_durable'), "0")
        self.assertEquals(self.conn.query_timestamp('get=last_checkpoint'), "0")
        self.assertEquals(self.conn.query_timestamp('get=oldest'), "0")
        self.assertEquals(self.conn.query_timestamp('get=oldest_reader'), "0")
        self.assertEquals(self.conn.query_timestamp('get=oldest_timestamp'), "0")
        self.assertEquals(self.conn.query_timestamp('get=pinned'), "0")
        self.assertEquals(self.conn.query_timestamp('get=recovery'), "0")
        self.assertEquals(self.conn.query_timestamp('get=stable'), "0")
        self.assertEquals(self.conn.query_timestamp('get=stable_timestamp'), "0")

    def test_session_query_notset(self):
        self.assertEquals(self.session.query_timestamp('get=commit'), "0")
        self.assertEquals(self.session.query_timestamp('get=first_commit'), "0")
        self.assertEquals(self.session.query_timestamp('get=prepare'), "0")
        self.assertEquals(self.session.query_timestamp('get=read'), "0")

if __name__ == '__main__':
    wttest.run()
