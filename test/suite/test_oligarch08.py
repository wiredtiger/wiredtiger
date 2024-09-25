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

import wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_oligarch08.py
# Simple read write testing using the page log API

class test_oligarch08(wttest.WiredTigerTestCase, DisaggConfigMixin):

    conn_base_config = 'log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'oligarch=(role="leader")'
    disagg_storages = gen_disagg_storages('test_oligarch08', disagg_only = True)

    # Make scenarios for different cloud service providers
    scenarios = make_scenarios(disagg_storages)

    nitems = 100

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_oligarch_read_write(self):
        uri = "oligarch:test_oligarch08"
        create_session_config = 'key_format=S,value_format=S,stable_prefix=.,page_log=palm'
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
        
        self.session.checkpoint()

        #self.reopen_conn()

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            self.assertEquals(cursor["Hello " + str(i)], "World")
