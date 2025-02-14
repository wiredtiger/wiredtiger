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

# test_layered28.py
#    Step a node up without a stable table, and ensure it creates one.
@disagg_test_class
class test_layered28(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 5000

    conn_base_config = 'checkpoint=(precise=true),disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="follower")'

    table_config = 'key_format=Q,value_format=S'

    uri = "layered:test_layered28"

    disagg_storages = gen_disagg_storages('test_layered28', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Custom test case setup
    def early_setup(self):
        os.mkdir('kv_home')
        os.mkdir('follower')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    def test_layered28(self):
        # Create a table + insert data while still a follower
        self.session.create(self.uri, self.table_config)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(self.nitems):
            cursor[i] = 'hello ' + str(i)
        cursor.close()
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        # Step up
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Create a checkpoint
        self.session.checkpoint()

        # Start a follower and make sure it sees the data too
