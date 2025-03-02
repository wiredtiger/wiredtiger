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

import os, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered32.py
#    Test tombstone value on the ingest table works as expected.
@disagg_test_class
class test_layered32(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="follower")'

    create_session_config = 'key_format=S,value_format=S'

    disagg_storages = gen_disagg_storages('test_layered32', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def add_key(self, uri, key, value):
        cursor = self.session.open_cursor(uri, None, None)
        cursor[key] = value
        cursor.close()

    def verify_key_exists(self, uri, key, value):
        cursor = self.session.open_cursor(uri, None, None)
        cursor.set_key(key)
        cursor.search()
        self.assertEqual(value, cursor.get_value())
        cursor.close()

    # Put some special values that start with the layered tombstone
    def test_layered32(self):
        uri = "layered:test_layered32"
        self.session.create(uri, 'key_format=S,value_format=u')
        v = b'\x14\x14'
        self.add_key(uri, 'k1', v)
        self.verify_key_exists(uri, 'k1', v)
        v = b'\x14\x14\0\0\0\0\0\0'
        self.add_key(uri, 'k2', v)
        self.verify_key_exists(uri, 'k2', v)
        v += b'a' * 1000
        self.add_key(uri, 'k3', v)
        self.verify_key_exists(uri, 'k3', v)
