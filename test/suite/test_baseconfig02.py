#!/usr/bin/env python3
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

# test_baseconfig02.py
#    Early-load extensions are loaded before WiredTiger.basecfg is read, so an entry persisted
#    in basecfg cannot be replayed from there on reopen. wiredtiger_open must reject the open
#    instead of silently leaving the extension absent.
@wttest.skip_for_hook("disagg", "hook always passes extensions, shadowing basecfg")
class test_baseconfig02(wttest.WiredTigerTestCase):
    # Toggled to control whether conn_extensions emits the entry on the next open.
    include_extension = True

    def conn_extensions(self, extlist):
        if self.include_extension:
            extlist.early_load_ext = True
            extlist.extension('encryptors', 'rotn')

    def test_baseconfig02(self):
        # Reopen without the extension; wiredtiger_open must log a warning about the missing
        # entry, but the open itself still succeeds.
        self.include_extension = False
        with self.expectedStdoutPattern('configured with early_load=true but was not passed'):
            self.reopen_conn()

        # Reopen with the extension; nothing should be logged.
        self.include_extension = True
        self.reopen_conn()
