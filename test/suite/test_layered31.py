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

# We support using the reset operation to pick up the latest checkpoint for layered: URIs.
# In milestone 4, however, layered URIs are not used, and the way we pick up the latest checkpoint
# is a bit hacky.  We still want to verify that this technique works, at least for a while.
# The milestone 4 specific code is marked by this variable:
test_milestone4 = True

# test_layered31.py
#    Extra tests for follower picking up new checkpoints.
@disagg_test_class
class test_layered31(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    layered_uris = ["layered:test_layered31a", "layered:test_layered31b"]
    all_uris = list(layered_uris)
    if test_milestone4:
        other_uris = ["file:test_layered31c", "table:test_layered31d"]
        all_uris += other_uris

    disagg_storages = gen_disagg_storages('test_layered31', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

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
    def test_layered31(self):
        # Create all tables in the leader
        for uri in self.all_uris:
            cfg = self.create_session_config
            if not uri.startswith('layered'):
                cfg += ',block_manager=disagg,log=(enabled=false)'
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
            cursor.close()

        self.session.checkpoint()

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            cursor = self.open_follow_cursor(session_follow, uri)
            for i in range(self.nitems):
                self.assertEqual(cursor[str(i)], value_prefix0 + str(i))
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
            cursor.close()

        self.session.checkpoint()

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Open a cursor')
            cursor = self.open_follow_cursor(session_follow, uri)
            follower_cursors[uri] = cursor
            for i in range(self.nitems):
                self.assertEqual(cursor[str(i)], value_prefix1 + str(i))

        #
        # Part 2: Close and reopen the cursor after picking up a checkpoint.
        #

        # Put data to all tables, version 2
        value_prefix2 = 'bbb'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix2 + str(i)
            cursor.close()

        self.session.checkpoint()

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Close and reopen the cursor')
            if uri in follower_cursors:
                follower_cursors[uri].close()
            cursor = self.open_follow_cursor(session_follow, uri)
            follower_cursors[uri] = cursor
            for i in range(self.nitems):
                self.assertEqual(cursor[str(i)], value_prefix2 + str(i))

        #
        # Part 3: Reset the cursor after picking up a checkpoint.
        #

        # Put data to all tables, version 3
        value_prefix3 = 'ccc'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix3 + str(i)
            cursor.close()

        self.session.checkpoint()

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        for uri in self.all_uris:
            self.pr(f'{uri}: Reset and reuse the cursor')
            cursor = follower_cursors[uri]
            cursor = self.reset_follow_cursor(cursor)
            for i in range(self.nitems):
                self.assertEqual(cursor[str(i)], value_prefix3 + str(i))

        #
        # Part 4: Check that an open cursor's position
        # does not change after picking up a checkpoint.
        #

        # In scanning, do a first batch, reading half the items.
        first_read = self.nitems // 2

        # Stringized integers (the keys used) are not in the same order as integers.
        # Make a list of the keys in order so we can verify the results from scanning.
        keys_in_order = sorted([str(k) for k in range(self.nitems)])

        # On the follower, scan and check the first half, leaving cursors open
        for uri in self.all_uris:
            self.pr(f'{uri}: Reset the cursor and scan the first half of items')
            cursor = follower_cursors[uri]
            cursor = self.reset_follow_cursor(cursor)
            found = 0

            # Scan the first half of the items.
            for i in range(first_read):
                ret = cursor.next()
                if ret == wiredtiger.WT_NOTFOUND:
                    break
                self.assertEqual(ret, 0)
                expected_key = keys_in_order[i]
                self.assertEqual(cursor.get_key(), expected_key)
                self.assertEqual(cursor.get_value(), value_prefix3 + expected_key)
                found += 1

        # Make a change on the leader, and propogate to the follower, so that the
        # follower cursors reopen.
        value_prefix4 = 'ddd'
        for uri in self.all_uris:
            cursor = self.session.open_cursor(uri, None, None)
            for i in range(self.nitems):
                cursor[str(i)] = value_prefix4 + str(i)
            cursor.close()

        self.session.checkpoint()
        self.pr(f'{uri}: Advance the checkpoint')
        self.disagg_advance_checkpoint(conn_follow)

        # Check the continuation of each scan in the follower.
        # Note that we are checking with layered URIs only.
        # Non-layered URIs in this test have a hack (in reset_follow_cursors)
        # that reopens those cursors, thus losing their position.
        for uri in self.layered_uris:
            self.pr(f'{uri}: continue reading items, expecting to see the second half')
            cursor = follower_cursors[uri]
            found = first_read
            for i in range(first_read, self.nitems):
                ret = cursor.next()
                if ret == wiredtiger.WT_NOTFOUND:
                    break
                self.assertEqual(ret, 0)

                # We're checking that we haven't lost our place in the key space.
                # For the value, we're only checking that the prefix contains the
                # key, as it always does in this test. As for which value, the old
                # or the new, we'll accept either in this test. We don't want to
                # assume to know exactly when the cursor is promoted on the follower.
                #
                # At the moment, this is the behavior of follower cursors,
                # that if reading without a timestamp, they may give the most recent
                # result.  Mongodb will always reads with a timestamp.
                # TODO: consider "fixing" this, by only promoting follower cursors if they
                # are reading at a timestamp, or at specific points, like Cursor.reset
                expected_key = keys_in_order[i]
                self.assertEqual(cursor.get_key(), expected_key)
                current_value = cursor.get_value()
                self.assertTrue(current_value == value_prefix3 + expected_key or \
                                current_value == value_prefix4 + expected_key)
                found += 1
            self.assertEqual(found, self.nitems)

        # Clean up
        for cursor in follower_cursors.values():
            cursor.close()

        session_follow.close()
        conn_follow.close()
