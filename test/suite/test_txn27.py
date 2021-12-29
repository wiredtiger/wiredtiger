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
from wtdataset import SimpleDataSet

# test_txn27.py
#   Test that rollback sets the reason.
class test_txn27(wttest.WiredTigerTestCase):
    def test_rollback_reason(self):
        uri = "table:txn27"
        ds = SimpleDataSet(self, uri, 10, key_format='S', value_format='S')
        ds.populate()

        s1 = self.session
        c1 = s1.open_cursor(uri)
        s1.begin_transaction()
        c1[ds.key(5)] = "aaa"

        s2 = self.conn.open_session()
        c2 = s2.open_cursor(uri)
        s2.begin_transaction()
        c2.set_key(ds.key(5))
        c2.set_value("bbb")
        msg = '/conflict between concurrent operations/'
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: c2.update(), msg)
        self.assertEquals('/' + s2.get_rollback_reason() + '/', msg)

if __name__ == '__main__':
    wttest.run()
