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

import wttest, wiredtiger

# test_layered_fast_truncate19.py
#   Validate the debug_mode.disagg_slow_truncate_follower connection config
#   (boolean), and that reconfigure accepts toggling it.
class test_layered_fast_truncate19(wttest.WiredTigerTestCase):
    def test_open_accepts_true(self):
        self.reopen_conn(config='debug_mode=(disagg_slow_truncate_follower=true)')

    def test_open_accepts_false(self):
        self.reopen_conn(config='debug_mode=(disagg_slow_truncate_follower=false)')

    def test_open_default(self):
        # Default omits the knob entirely.
        self.reopen_conn(config='')

    def test_reconfigure_toggle(self):
        self.conn.reconfigure('debug_mode=(disagg_slow_truncate_follower=true)')
        self.conn.reconfigure('debug_mode=(disagg_slow_truncate_follower=false)')

    def test_reconfigure_rejects_invalid(self):
        with self.expectedStderrPattern("expected a boolean"):
            self.assertRaisesException(
                wiredtiger.WiredTigerError,
                lambda: self.conn.reconfigure(
                    'debug_mode=(disagg_slow_truncate_follower=bogus)'))
