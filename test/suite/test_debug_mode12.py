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

# Test that the overwrite_free debug mode setting is accepted, can be changed on a running
# database, and leaves normal operation intact while the structures it covers are released.
#
# This does not verify that the byte pattern is written. That happens immediately before the block
# is handed back to the allocator, so it cannot be observed from here without reading freed memory.
# The fill itself is covered by the "[poison]" Catch2 tests.
class test_debug_mode12(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f"table:{test_name}"
    nrows = 10000

    # Keep the cache small so pages are evicted and their disk images released.
    conn_config = 'cache_size=2MB,debug_mode=(overwrite_free=true)'

    def value(self, i):
        return f'value{i}' * 20

    def test_overwrite_free_does_not_disturb_data(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[i] = self.value(i)
        c.close()
        self.session.checkpoint()

        # Repeated cursor and handle open/close churn releases the cursor, data handle and
        # data handle cache structures that the setting covers.
        for _ in range(20):
            c = self.session.open_cursor(self.uri)
            count = 0
            while c.next() == 0:
                self.assertEqual(c.get_value(), self.value(c.get_key()))
                count += 1
            self.assertEqual(count, self.nrows)
            c.close()
            self.session.reset()

        self.reopen_conn()

        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c.set_key(i)
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), self.value(i))
        c.close()

    # The setting can be changed on a running database, so that it can be turned on without a restart.
    def test_overwrite_free_reconfigure(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.conn.reconfigure('debug_mode=(overwrite_free=false)')

        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[i] = self.value(i)
        c.close()

        self.conn.reconfigure('debug_mode=(overwrite_free=true)')

        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c.set_key(i)
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), self.value(i))
        c.close()
