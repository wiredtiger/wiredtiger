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

# test_btree_usage02.py
#   WT-17717: the sweep server's btree-usage snapshot pass walks the open
#   dhandle list and dereferences each btree. Exercise that walk against heavy
#   handle churn (create/drop) with an aggressive sweep, while concurrently
#   reading the connection statistics cursor. A handle being closed out from
#   under the walk used to crash here (NULL btree dereference); the walk now
#   holds the dhandle list read lock, so this must run cleanly.

import wttest

class test_btree_usage02(wttest.WiredTigerTestCase):
    # Aggressive sweep: scan every second and close idle handles almost
    # immediately, so the usage-collection pass races with handle teardown.
    conn_config = 'statistics=(fast),' + \
        'file_manager=(close_scan_interval=1,close_idle_time=1,close_handle_minimum=0)'

    def read_stats(self):
        # Iterate the whole connection statistics cursor, including the appended
        # btree-usage virtual entries. The point is to drive the read path, not
        # to assert values.
        c = self.session.open_cursor('statistics:', None, None)
        try:
            while c.next() == 0:
                c.get_value()
        finally:
            c.close()

    def test_handle_churn(self):
        value = 'v' * 40
        # Many short-lived tables: create, populate, read stats, drop. The
        # aggressive sweep closes idle handles between iterations, so the
        # background usage-collection pass walks a list whose entries are being
        # torn down concurrently.
        for i in range(200):
            uri = 'table:bu02_%d' % i
            self.session.create(uri, 'key_format=S,value_format=S')
            cursor = self.session.open_cursor(uri, None, None)
            for k in range(50):
                cursor['%08d' % k] = value
            cursor.close()

            # Read the usage-bearing connection stats while sweep is active.
            self.read_stats()

            # Drop older tables to keep churning the handle list.
            if i >= 8:
                self.dropUntilSuccess(self.session, 'table:bu02_%d' % (i - 8))

        # Surviving completion (no crash/hang) is the assertion. Do a final read.
        self.read_stats()
