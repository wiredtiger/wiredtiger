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
#
# test_bulk03.py
#       bulk-cursor test.
#

import os
import wiredtiger, wttest
from wtdataset import SimpleDataSet, simple_key, simple_value
from wtscenario import make_scenarios

# Smoke test bulk-load.
class test_bulk_load(wttest.WiredTigerTestCase):
    extension_name = 'local_store'
    name = 'test_bulk'

    cursor_types = [
        ('file', dict(type='file:', msg='/bulk-load is only supported on newly created objects/')),
        ('lsm', dict(type='lsm:', msg='/bulk-load is only supported on newly created LSM trees/')),
        ('table', dict(type='table:', msg='/bulk-load is only supported on newly created objects/'))
    ]

    conn_types = [
        ('non_tiered', dict(is_tiered=False)),
        ('tiered', dict(is_tiered=True))
    ]

    scenarios = make_scenarios(cursor_types, conn_types)

    def conn_config(self):
        config = ""
        if self.is_tiered:
            if not os.path.exists('mybucket'):
                os.mkdir('mybucket')
            config = 'tiered_storage=(auth_token=test_token,bucket=mybucket,bucket_prefix=pfx-,' + \
                'name=%s),tiered_manager=(wait=0)' % self.extension_name

        return config

    # Load the local store extension, but skip the test if it is missing.
    def conn_extensions(self, extlist):
        if self.is_tiered:
            extlist.skip_if_missing = True
            extlist.extension('storage_sources', self.extension_name)

    # Test that bulk-load objects cannot be opened by other cursors.
    def test_bulk_load_busy(self):
        uri = self.type + self.name
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri, None)

        # Don't close the insert cursor, we want EBUSY.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(uri, None, "bulk"), self.msg)

if __name__ == '__main__':
    wttest.run()
