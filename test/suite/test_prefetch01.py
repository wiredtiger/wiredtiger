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

import os
import helper, wiredtiger, wttest

# test_prefetch01.py
#    Test basic functionality of the prefetch configuration.
class test_prefetch01(wttest.WiredTigerTestCase):
    conn_config = 'prefetch=(available=true,default=false)'

    def test_prefetch_config(self):
        s = self.conn.open_session('prefetch=(enabled=true)')
        self.assertEqual(s.close(), 0)
        self.close_conn()

# Test that pre-fetching cannot be enabled for sessions when the pre-fetching
# functionality is not available.
class test_prefetch_incompatible_config(wttest.WiredTigerTestCase):
    new_dir = 'new.dir'

    # Open a new connection as WiredTiger does not support re-configuring
    # pre-fetch during runtime.
    def test_incompatible_config(self):
        os.mkdir(self.new_dir)
        helper.copy_wiredtiger_home(self, '.', self.new_dir)
        msg = '/pre-fetching cannot be enabled/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, 
            lambda: self.wiredtiger_open(self.new_dir, 'prefetch=(available=false,default=true)'), msg)

if __name__ == '__main__':
    wttest.run()
