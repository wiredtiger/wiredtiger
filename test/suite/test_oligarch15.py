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

import os, os.path, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_oligarch15.py
#    Start without local files.
class test_oligarch15(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_config = 'oligarch_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                + 'disaggregated=(stable_prefix=.,page_log=palm,role="follower"),'

    create_session_config = 'key_format=S,value_format=S'

    oligarch_uris = ["oligarch:test_oligarch15a", "oligarch:test_oligarch15b"]
    other_uris = ["file:test_oligarch15c", "table:test_oligarch15d"]

    disagg_storages = gen_disagg_storages('test_oligarch15', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Custom test case setup
    def early_setup(self):
        os.mkdir('kv_home')

    # Test starting without local files.
    def test_oligarch14(self):
        # The node started as a follower, so step it up as the leader
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Create tables
        for uri in self.oligarch_uris + self.other_uris:
            cfg = self.create_session_config
            if not uri.startswith('oligarch'):
                cfg += ',block_manager=disagg,oligarch_log=(enabled=false),log=(enabled=false)'
            self.session.create(uri, cfg)

        # Put data to tables
        value_prefix = 'aaa'
        for uri in self.oligarch_uris + self.other_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)
        checkpoint_id = self.disagg_get_complete_checkpoint()

        # Clean up
        self.close_conn()

        # Remove local files
        for f in os.listdir():
            if os.path.isdir(f):
                continue
            if f.startswith('WiredTiger') or f.startswith('test_'):
                os.remove(f)

        # Reopen the connection
        self.open_conn()

        # Recreate oligarch tables
        # FIXME-SLS-496 Remove this after we can create oligarch table metadata automatically
        for uri in self.oligarch_uris:
            cfg = self.create_session_config
            self.session.create(uri, cfg)

        # Pick up the checkpoint
        self.conn.reconfigure(f'disaggregated=(checkpoint_id={checkpoint_id})')

        # Become the leader (skip a few extra checkpoint IDs just in case)
        self.conn.reconfigure(f'disaggregated=(role="leader",next_checkpoint_id={checkpoint_id+2})')

        # Check tables after the restart
        for uri in self.oligarch_uris + self.other_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix + str(i))
            cursor.close()
