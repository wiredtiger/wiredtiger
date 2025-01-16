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

import os, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered10.py
#    Additional layered table & cursor methods.
@disagg_test_class
class test_layered10(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 100_000

    conn_base_config = 'layered_table_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    uri = "layered:test_layered10"

    disagg_storages = gen_disagg_storages('test_layered10', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    # Test additional layered table / cursor operations
    def test_layered10(self):
        session_config = 'key_format=i,value_format=S'

        self.session.create(self.uri, session_config)

        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + "disaggregated=(role=\"follower\")")
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, session_config)

        # Check the largest key before populating the data
        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(cursor.largest_key(), wiredtiger.WT_NOTFOUND)

        # Populate
        for i in range(self.nitems):
            cursor[i + 1] = f'Value {i}'

        # Check the largest key
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), self.nitems)

        # Check the largest key again after a checkpoint and a bit of a wait
        time.sleep(5)
        self.session.checkpoint()
        time.sleep(1)
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), self.nitems)

        self.session.begin_transaction()
        cursor[self.nitems + 100] = f'Value'

        # Check the largest key before commit
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), self.nitems + 100)
        self.session.rollback_transaction()

        # Ensure that all data makes it to the follower
        self.disagg_advance_checkpoint(conn_follow)

        # Check the largest key at the follower
        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        self.assertEqual(cursor_follow.largest_key(), 0)
        self.assertEqual(cursor_follow.get_key(), self.nitems)

        # Cleanup
        cursor.close()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()
