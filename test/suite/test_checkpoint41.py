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
# test_checkpoint41.py
#   A checkpoint taken while another session has created a table and inserted into it in an
#   unresolved transaction succeeds, and after a restart the table holds the insert only if the
#   transaction committed.

import wttest
from wiredtiger import WT_NOTFOUND
from wtscenario import make_scenarios

class test_checkpoint41(wttest.WiredTigerTestCase):
    uri = 'table:test_checkpoint41'
    create_cfg = 'key_format=i,value_format=S'
    key = 1
    value = 'value'

    scenarios = make_scenarios([
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ])

    def test_checkpoint_schema_in_txn(self):
        session2 = self.conn.open_session()
        session2.begin_transaction()
        session2.create(self.uri, self.create_cfg)
        cursor = session2.open_cursor(self.uri)
        cursor[self.key] = self.value
        cursor.close()

        # Checkpoint while the create and the insert are not resolved.
        self.session.checkpoint()

        if self.commit:
            session2.commit_transaction()
        else:
            session2.rollback_transaction()
        session2.close()
        self.session.checkpoint()

        self.reopen_conn()
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(self.key)
        if self.commit:
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), self.value)
        else:
            self.assertEqual(cursor.search(), WT_NOTFOUND)
        cursor.close()
