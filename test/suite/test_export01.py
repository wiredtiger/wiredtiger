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
#

from helper import copy_wiredtiger_home
from helper_tiered import TieredConfigMixin, tiered_storage_sources
from wtscenario import make_scenarios
import os, wttest

class test_export01(TieredConfigMixin, wttest.WiredTigerTestCase):
    dir = 'backup.dir'

    types = [
        ('table', dict(type = 'table:')),
    ]

    scenarios = make_scenarios(tiered_storage_sources, types)

    def test_export(self):
        uri_a = self.type + "exporta"
        uri_b = self.type + "exportb"
        uri_c = self.type + "exportc"

        # Create a few tables.
        self.session.create(uri_a)
        self.session.create(uri_b)
        self.session.create(uri_c)

        # Insert some records.
        c1 = self.session.open_cursor(uri_a)
        c1["k1"] = "v1"
        c1.close()

        c2 = self.session.open_cursor(uri_b)
        c2["k2"] = "k2"
        c2.close()

        c3 = self.session.open_cursor(uri_c)
        c3["k3"] = "k3"
        c3.close()

        # TODO: Flush stuff.

        # Open a special backup cursor for export operation.
        export_cursor = self.session.open_cursor('backup:export', None, None)

        os.mkdir(self.dir)
        copy_wiredtiger_home(self, '.', self.dir)

        # TODO: Check that the WiredTiger.export file contains the expected entries.
        # TODO: Remove the backup file properly.

        export_cursor.close()

if __name__ == '__main__':
    wttest.run()