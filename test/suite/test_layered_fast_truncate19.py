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
#   Validate the WT_SESSION.truncate "mode" config (fast | slow), and that an
#   invalid value is rejected by the parser.
class test_layered_fast_truncate19(wttest.WiredTigerTestCase):
    uri = 'table:test_layered_fast_truncate19'

    def truncate_with(self, mode_cfg):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        c1 = self.session.open_cursor(self.uri)
        c1.set_key("k00010")
        c2 = self.session.open_cursor(self.uri)
        c2.set_key("k00020")
        try:
            self.session.truncate(None, c1, c2, mode_cfg)
        finally:
            c1.close()
            c2.close()

    def test_mode_fast(self):
        self.truncate_with("mode=fast")

    def test_mode_slow(self):
        self.truncate_with("mode=slow")

    def test_mode_default(self):
        self.truncate_with(None)

    def test_mode_invalid(self):
        with self.expectedStderrPattern("not a permitted choice for key 'mode'"):
            self.assertRaisesException(
                wiredtiger.WiredTigerError,
                lambda: self.truncate_with("mode=bogus"))
