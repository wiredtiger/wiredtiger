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
# test_tiered15.py
#   Test the "type" configuration in session.create with tiered storage.

from helper_tiered import generate_s3_prefix, get_auth_token, get_bucket1_name, TieredConfigMixin, tiered_storage_sources
from wtscenario import make_scenarios
import wiredtiger, wttest

class test_tiered15(TieredConfigMixin, wttest.WiredTigerTestCase):
    uri_a = "table:tiereda"
    uri_b = "table:tieredb"
    uri_c = "table:tieredc"
    uri_d = "table:tieredd"
    uri_e = "table:tierede"
    uri_f = "table:tieredf"
    uri_g = "table:tieredg"
    uri_h = "table:tieredh"

    nt_uri_a = "table:nontiereda"
    nt_uri_b = "table:nontieredb"
    nt_uri_c = "table:nontieredc"
    nt_uri_d = "table:nontieredd"
    nt_uri_e = "table:nontierede"
    nt_uri_f = "table:nontieredf"
    nt_uri_g = "table:nontieredg"
    nt_uri_h = "table:nontieredh"

    types = [
        ('table', dict(type = 'table:')),
    ]

    scenarios = make_scenarios(tiered_storage_sources, types)

    def test_create_type_config(self):
        errmsg = "/Operation not supported/"
        if not self.is_tiered_scenario():
            self.skipTest('The test should only test connections configured with tiered storage.')

        # Creating a tiered table with the type configuration set to file should succeed.
        self.session.create(self.uri_a, "type=file")

        # Creating tiered tables when the type configuration is set to other types besides "file" should not succeed.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_b, "type=table"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_c, "type=tier"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_d, "type=tiered"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_e, "type=colgroup"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_f, "type=index"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_g, "type=lsm"), errmsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.uri_h, "type=backup"), errmsg)

        # Creating a non-tiered table within a connection configured with tiered storage should allow the
        # type configuration to be set to some other types besides "file". The types "tiered", "colgroup",
        # "index", and "backup" are not supported when creating a non-tiered table in a non-tiered connection.
        # Additionally, configuring type to "colgroup" causes the test to crash.
        # It is expected these types will also not work with non-tiered tables in a connection configured with tiered storage.
        self.session.create(self.nt_uri_a, "tiered_storage=(name=none),type=file")
        self.session.create(self.nt_uri_b, "tiered_storage=(name=none),type=table")
        self.session.create(self.nt_uri_c, "tiered_storage=(name=none),type=tier")
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.nt_uri_d, "tiered_storage=(name=none),type=tiered"), "/Invalid argument/")
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.nt_uri_f, "tiered_storage=(name=none),type=index"), "/Invalid argument/")
        self.session.create(self.nt_uri_g, "tiered_storage=(name=none),type=lsm")
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.session.create(self.nt_uri_h, "tiered_storage=(name=none),type=backup"), errmsg)

if __name__ == '__main__':
    wttest.run()
