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

# test_txn26.py
#   Test that commit should fail if commit timestamp is smaller or equal to the active timestamp. Our handling of out of order timestamp relies on this to ensure repeated reads are working as expected.
def timestamp_str(t):
    return '%x' % t
class test_txn26(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'

    def test_commit_larger_than_active_timestamp(self):
        if not wiredtiger.diagnostic_build():
            self.skipTest('requires a diagnostic build')

        uri = 'table:test_txn26'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value = 'a'

        # Start a session with timestamp 10
        session2 = self.conn.open_session(self.session_config)
        session2.begin_transaction('read_timestamp=' + timestamp_str(10))

        # Try to commit at timestamp 10
        self.session.begin_transaction()
        cursor[str(0)] = value
        with self.expectedStderrPattern("must be greater than the latest active read timestamp"):
            try:
                self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))
            except wiredtiger.WiredTigerError as e:
                gotException = True
                self.pr('got expected exception: ' + str(e))
                self.assertTrue(str(e).find('nvalid argument') >= 0)
        self.assertTrue(gotException, msg = 'expected exception')
