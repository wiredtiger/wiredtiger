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

# test_eviction01.py
# Ensure the application thread is not waiting forever for cache space if 
# cache_max_wait_ms is set
class test_eviction01(wttest.WiredTigerTestCase):
    # Force a very small cache.
    conn_config = 'cache_size=1MB'

    def test(self):
        uri = "table:test_eviction01"
        self.session.create(uri, 'key_format=i,value_format=S')

        session2 = self.conn.open_session('cache_max_wait_ms=1000')
        cursor = session2.open_cursor(uri)

        # Do a large transaction
        session2.begin_transaction()
        rollback = False
        for i in range(0, 10000):
            cursor.set_key(i)
            cursor.set_value('a' * 1024)
            try:
                cursor.insert()
            except:
                session2.rollback_transaction()
                rollback = True
                break

        # We must have rollbacked the transaction because of cache pressure
        self.assertEquals(rollback, True)
        self.ignoreStdoutPatternIfExists("oldest pinned transaction ID rolled back for eviction")
