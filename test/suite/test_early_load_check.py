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
import wiredtiger, wttest

# test_early_load_check.py
#     WT-17691: wiredtiger_open must reject an open when an early_load=true extension
#     recorded in WiredTiger.basecfg was not also passed in the open configuration.
class test_early_load_check(wttest.WiredTigerTestCase):
    # Toggled between opens to control whether conn_extensions emits the lz4 entry.
    include_extension = True

    def conn_extensions(self, extlist):
        if self.include_extension:
            extlist.skip_if_missing = True
            extlist.early_load_ext = True
            extlist.extension('compressors', 'lz4')

    def test_early_load_check(self):
        # First open recorded lz4 with early_load=true in WiredTiger.basecfg.
        # Reopen with no extensions: the guardrail in wiredtiger_open must reject.
        self.include_extension = False
        with self.expectedStderrPattern('configured with early_load=true but was not passed'):
            self.assertRaises(wiredtiger.WiredTigerError, lambda: self.reopen_conn())

        # Reopen with the extension passed back in: the open must succeed.
        self.include_extension = True
        self.reopen_conn()
