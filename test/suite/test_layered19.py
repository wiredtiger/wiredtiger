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

# We support using the reset operation to pick up the latest checkpoint for layered: URIs.
# In milestone 4, however, layered URIs are not used, and the way we pick up the latest checkpoint
# is a bit hacky.  We still want to verify that this technique works, at least for a while.
# The milestone 4 specific code is marked by this variable:
test_milestone4 = True

# test_layered19.py
#    Extra tests for follower picking up new checkpoints.
class test_layered19(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_base_config = 'layered_table_log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    layered_uris = ["layered:test_layered19a", "layered:test_layered19b"]
    all_uris = layered_uris
    if test_milestone4:
        other_uris = ["file:test_layered19c", "table:test_layered19d"]
        all_uris += other_uris

    disagg_storages = gen_disagg_storages('test_layered19', disagg_only = True)
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

    # Open a cursor on the follower.  Generally, the test will open a layered: uri.
    # But we want to temporarily support a world in M4 where we must directly open the
    # stable table, using a file: or table: . This is needed because in this world we don't
    # pre-create the ingest tables needed to use a layered: uri.
    def open_follow_cursor(self, session, uri):
        config = None
        if test_milestone4:
            if uri.startswith('table:') or uri.startswith('file:'):
                #self.tty(f'\n\n**** OPENING FOLLOW CURSOR {uri} with force=true ****\n')
                config = 'force=true'
        return session.open_cursor(uri, None, config)

    # Reset a cursor on the follower.  Generally, the test will open a layered: uri,
    # and a reset is a signal have the cursor move to the next checkpoint. This works
    # for layered cursors but not cursors in general.  In the m4 milestone where we don't
    # use a layered cursor, to get similar behavior, we need to reopen the cursor.
    def reset_follow_cursor(self, cursor):
        if test_milestone4:
            uri = cursor.uri
            if uri.startswith('table:') or uri.startswith('file:'):
                #self.tty(f'\n\n**** INSTEAD OF RESET, REOPEN FOLLOW CURSOR {uri} with force=true ****\n')
                session = cursor.session
                cursor = session.open_cursor(uri, None, 'force=true')
                return cursor
        cursor.reset()
        return cursor

    # Test more than one table.
    def test_layered19(self):
        # Create all tables in the leader
        for uri in self.all_uris:
            cfg = self.create_session_config
            if not uri.startswith('layered'):
                cfg += ',block_manager=disagg,layered_table_log=(enabled=false),log=(enabled=false)'
            self.session.create(uri, cfg)

        # Create the follower
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        follower_cursors = dict()

        #
        # Setup: Check data in the follower normally.
        #

        # Put data to all tables, version 0
        value_prefix0 = '---'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix0 + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            cursor = self.open_follow_cursor(session_follow, uri)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix0 + str(i))
            cursor.close()

        #
        # Part 1: Check data in the follower, but keep the cursors open.
        #

        # Put data to all tables, version 1
        value_prefix1 = 'aaa'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix1 + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Open a cursor')
            cursor = self.open_follow_cursor(session_follow, uri)
            follower_cursors[uri] = cursor
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix1 + str(i))

        #
        # Part 2: Close and reopen the cursor after picking up a checkpoint.
        #

        # Put data to all tables, version 2
        value_prefix2 = 'bbb'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix2 + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Close and reopen the cursor')
            if uri in follower_cursors:
                follower_cursors[uri].close()
            cursor = self.open_follow_cursor(session_follow, uri)
            follower_cursors[uri] = cursor
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix2 + str(i))

        #
        # Part 3: Reset the cursor after picking up a checkpoint.
        #

        # Put data to all tables, version 3
        value_prefix3 = 'ccc'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix3 + str(i)
                if i % 250 == 0:
                    time.sleep(1)
            cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Reset and reuse the cursor')
            cursor = follower_cursors[uri]
            cursor = self.reset_follow_cursor(cursor)
            for i in range(self.nitems):
                self.assertEquals(cursor[str(i)], value_prefix3 + str(i))

        # Clean up
        for cursor in follower_cursors.values():
            cursor.close()

        session_follow.close()
        conn_follow.close()
