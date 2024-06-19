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

import wiredtiger, wttest, time

# test_txn27.py
#   Test that the API returning a rollback error sets the reason for the rollback.
class test_txn27(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=100MB, max_transaction_modify_count=1000'

    def test_rollback_reason(self):
        uri = "table:txn28"
        # Create and populate a table.
        table_params = 'key_format=i,value_format=S'
        session = self.session
        session.create(uri, table_params)
        cursor = session.open_cursor(uri, None)
        for i in range(1, 1000):
            if i == 1000:
                msg = '/transaction rolled back because of big transaction/'
                self.assertEquals('/' + session.get_rollback_reason() + '/', msg)
            else:
                cursor[i] = str(i)
        cursor.close()
