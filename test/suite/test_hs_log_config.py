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
from wtscenario import make_scenarios

# test_hs_log_config.py
#    Check log disabled for the history store table in metadata.
class test_hs_log_config(wttest.WiredTigerTestCase):
    name = "test_hs_log_config"
    logging = [
        ('log-enabled', dict(logenabled=True)),
        ('log-disabled', dict(logenabled=False)),
    ]

    scenarios = make_scenarios(logging)

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def verify_metadata(self, metastr):
        c = self.session.open_cursor('metadata:', None, None)

        # We must find a file type entry for this object and its value
        # should contain the provided file meta string.
        c.set_key('file:WiredTigerHS.wt')
        self.assertNotEqual(c.search(), wiredtiger.WT_NOTFOUND)
        value = c.get_value()
        self.assertTrue(value.find(metastr) != -1)

        c.close()

    def test_hs_log_config(self):
        uri = "file:" + self.name
        entries = 100
        create_params = 'key_format=i,value_format=i,'

        if self.logenabled:
            log_param = "log=(enabled=true)"
        else:
            log_param = "log=(enabled=false)"

        self.session.create(uri, create_params + log_param)

        # Put some data in table.
        self.session.begin_transaction()
        c = self.session.open_cursor(uri, None)
        for k in range(entries):
            c[k+1] = 1
        self.session.commit_transaction()
        c.close()

        self.session.checkpoint()

        # Verify the string in the metadata.
        self.verify_metadata('log=(enabled=false)')
