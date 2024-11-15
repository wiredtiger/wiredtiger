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
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_oligarch13.py
#    More than one table.
class test_oligarch13(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_base_config = 'oligarch_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    oligarch_uris = ["oligarch:test_oligarch13a", "oligarch:test_oligarch13b"]
    other_uris = ["file:test_oligarch13c"]

    disagg_storages = gen_disagg_storages('test_oligarch08', disagg_only = True)
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

    # Test more than one table.
    def test_oligarch13(self):
        # Create all tables in the leader
        for uri in self.oligarch_uris + self.other_uris:
            cfg = self.create_session_config
            if not uri.startswith('oligarch'):
                cfg += ',block_manager=disagg,oligarch_log=(enabled=false),log=(enabled=false)'
            self.session.create(uri, cfg)

        # Create only the oligarch tables in the follower
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        for uri in self.oligarch_uris:
            cfg = self.create_session_config
            session_follow.create(uri, cfg)

        # Put data to all tables
        for uri in self.oligarch_uris + self.other_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = uri
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        # Check tables in the leader
        for uri in self.oligarch_uris + self.other_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], uri)
            cursor.close()

        # Pick up the checkpoint in the follower
        self.disagg_advance_checkpoint(conn_follow)

        # Check tables in the follower
        for uri in self.oligarch_uris + self.other_uris:
            # XXX Non-oligarch tables still have transaction IDs from the leader, so we need to set
            #     the isolation level to "read-uncommitted" to disable the visibility checks.
            if not uri.startswith('oligarch'):
                session_follow.begin_transaction('isolation="read-uncommitted"')
            cursor = session_follow.open_cursor(uri, None, None)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], uri)
            cursor.close()
            if not uri.startswith('oligarch'):
                session_follow.rollback_transaction()

        session_follow.close()
        conn_follow.close()
