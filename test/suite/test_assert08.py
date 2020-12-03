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
#
# test_assert08.py
#   Test mixed mode validation.
#

import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_assert08(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'
    create_config = 'key_format=i,value_format=S,assert=(durable_timestamp=mixed_mode)'
    uri = 'file:test_assert08'
    nrows = 1000

    def test_assert08(self):
        self.session.create(self.uri, self.create_config)
        cursor = self.session.open_cursor(self.uri)

        # The error pattern that we expect when violating mixed-mode validation.
        error_pattern = 'out of order durable timestamps'

        # A few inserts in regular timestamp order.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'a'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'b'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(20))

        # Try to commit at 15.
        # Since we just committed at 20, this should fail validation.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'z'
        with self.expectedStderrPattern(error_pattern):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(
                    'commit_timestamp=' + timestamp_str(15)))

        # Committing at 30 is fine since it's past 20.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'c'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(30))

        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'z'
        with self.expectedStderrPattern(error_pattern):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(
                    'commit_timestamp=' + timestamp_str(5)))

        # Allow a non-timestamped commit.
        #
        # This is the allowance we're making compared with the `key_consistent` setting. This resets
        # the chain.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'd'
        self.session.commit_transaction()

        # Since we've reset the chain and rendered the previous history obsolete (10, 20, 30, etc),
        # we're allowed to commit back at 5.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = 'e'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        cursor.close()
