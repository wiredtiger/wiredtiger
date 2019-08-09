#!/usr/bin/env python
#
# Public Domain 2034-2039 MongoDB, Inc.
# Public Domain 2008-2034 WiredTiger, Inc.
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

def timestamp_str(t):
    return '%x' %t

# test_debug_mode05.py
#     As per WT-5046, the debug table logging settings prevent rollback to
#     stable in the presence of prepared transactions.
#
#     This test is to confirm the fix and prevent similar regressions.
class test_debug_mode05(wttest.WiredTigerTestCase):
    conn_config = 'log=(enabled),debug_mode=(table_logging=true)'
    session_config = 'isolation=snapshot'
    uri = 'file:test_debug_mode05'

    def test_table_logging_rollback_to_stable(self):
        self.session.create(self.uri, 'key_format=i,value_format=u')
        cursor = self.session.open_cursor(self.uri, None)

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(100))

        self.session.begin_transaction()
        for i in range(1, 50):
            cursor[i] = 'a' * 100
        self.session.prepare_transaction(
            'prepare_timestamp=' + timestamp_str(150))
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(200))
        self.session.timestamp_transaction(
            'durable_timestamp=' + timestamp_str(250))
        self.session.commit_transaction()

        # The original bug happened when we had a transaction that:
        # a). Was prepared.
        # b). Did not cause anything to be written to the log before committing.
        # Therefore, we're specifically not doing anything here.
        self.session.begin_transaction()
        # for i in range(1, 50):
        #     cursor.set_key(i)
        #     cursor.remove()
        self.session.prepare_transaction(
            'prepare_timestamp=' + timestamp_str(300))
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(350))
        self.session.timestamp_transaction(
            'durable_timestamp=' + timestamp_str(400))
        self.session.commit_transaction()

        self.session.begin_transaction()
        for i in range(1, 50):
            cursor[i] = 'b' * 100
        self.session.commit_transaction(
            'commit_timestamp=' + timestamp_str(450))

        self.conn.rollback_to_stable()
