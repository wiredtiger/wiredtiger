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
# test_tiered17.py
#    Test that opening a file in readonly mode does not create a new object in tier.

from helper import copy_wiredtiger_home
from helper_tiered import TieredConfigMixin, gen_tiered_storage_sources, get_conn_config
from wtscenario import make_scenarios
import os, wiredtiger, wttest

class test_tiered17(TieredConfigMixin, wttest.WiredTigerTestCase):
    tiered_storage_sources = gen_tiered_storage_sources()
    saved_conn = ''
    uri = "table:test_tiered"
    base = "test_tiered-000000000"
    obj1file = base + "1.wtobj"
    obj2file = base + "2.wtobj"

    conn_readonly = [
        ('conn_readonly', dict(conn_readonly=True)),
        ('conn_writable', dict(conn_readonly=False)),
    ]

    shutdown = [
        ('clean', dict(clean=True)),
        ('unclean', dict(clean=False)),
    ]

    def conn_config(self):
        if self.is_tiered_scenario():
            self.saved_conn = get_conn_config(self) + ')'
        return self.saved_conn

    scenarios = make_scenarios(tiered_storage_sources, shutdown)

    def test_open_readonly_db(self):
        # Create and populate a table.
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        c["a"] = "a"
        c["b"] = "b"

        # Do a checkpoint and flush operation.
        self.session.checkpoint()
        self.session.flush_tier(None)

        # Add more data but don't do a checkpoint or flush in the unclean shutdown scenario.
        if not self.clean:
            c["c"] = "c"
            c["d"] = "d"
        
        c.close()

        if self.is_tiered_scenario():
            self.assertTrue(os.path.isfile(self.obj1file))
            self.assertTrue(os.path.isfile(self.obj2file))

        localobj = './' + self.obj1file
        if os.path.exists(localobj):
            os.remove(localobj)
        localobj = './' + self.obj2file
        if os.path.exists(localobj):
            os.remove(localobj)

        if self.conn_readonly:
            # Re-open the connection but in readonly mode.
            conn_params = 'readonly=true,' + self.saved_conn
        else:
            conn_params = self.saved_conn
            self.reopen_conn(config = conn_params)
            c2 = self.session.open_cursor(self.uri, None, "readonly=true")

            if self.is_tiered_scenario():
                self.assertFalse(os.path.isfile(self.obj1file))
                self.assertFalse(os.path.isfile(self.obj2file))

            c2.close()
            self.close_conn()

if __name__ == '__main__':
    wttest.run()
