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

import os, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered49.py
#    Test passing encryption keys to and from the PALI interface.
@disagg_test_class
class test_layered49(wttest.WiredTigerTestCase, DisaggConfigMixin):
    nitems = 500

    # The keys in this test are integer values less than nitems that have been "stringized".
    # Make an array of the keys in sort order so we can verify the results from scanning.
    keys_in_order = sorted([str(k) for k in range(nitems)])

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(page_log=palm),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    uri = "layered:test_layered49a"

    disagg_storages = gen_disagg_storages('test_layered49', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    # Load the page log extension, which has disaggregated storage support
    def conn_extensions(self, extlist):
        # Tell PALM to be verbose, and send that output to the WT
        # extension message API.  This guarantees that it will be captured
        # in the Python stdout file. Regular PALM verbose messages are not
        # captured.
        self.palm_debug = True
        self.palm_config = 'verbose_msg=1'

        if os.name == 'nt':
            extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def put_data(self, value_prefix, low = 0, high = nitems, session = None):
        if session == None:
            session = self.session   # leader by default
        cursor = session.open_cursor(self.uri, None, None)
        for i in range(low, high):
            cursor[str(i)] = value_prefix + str(i)
        cursor.close()

    def check_data(self, cursor, value_prefix, low = 0, high = nitems):
        for i in range(low, high):
            self.assertEqual(cursor[str(i)], value_prefix + str(i))

    # Scan data from low to high expecting to see all the keys and values using the given prefix.
    #
    # This function is sometimes called doing partial scans, and later, after a state change,
    # continuing using the same cursor.  We are promised that cursor iteration results aren't
    # affected by other transactions. Extending this reasoning to state changes, like picking up
    # checkpoints and stepping up to leader, cursors should similarly be unaffected by state
    # changes happening concurrently to the lifetime of the cursor.
    def scan_data(self, cursor, value_prefix, low = 0, high = nitems):
        if value_prefix == 'eee':
            self.session_follow.breakpoint()
        uri = self.uri

        found = 0
        for i in range(low, high):
            ret = cursor.next()
            if ret == wiredtiger.WT_NOTFOUND:
                break
            self.assertEqual(ret, 0)
            expected_key = self.keys_in_order[i]
            self.assertEqual(cursor.get_key(), expected_key)
            self.assertEqual(cursor.get_value(), value_prefix + expected_key)
            found += 1
        self.assertEqual(found, high - low)

    # This test was copied from layered31, but has been simplified a lot.
    # We want to establish a leader and follower, and have the follower
    # step up to a leader and make changes.  This guarantees (on the follower),
    # that page(s) have been read from disaggregated storage and that we
    # are making changes to those changes.  The pages we receieved from DS
    # have dek (encryption keys), and when we write deltas for those pages,
    # we want to make sure we use those encryption keys.  We check this by reading
    # the verbose output, looking for a message that we've used a saved key.
    #
    def test_layered49(self):
        cfg = self.create_session_config
        self.session.create(self.uri, cfg)

        # Create the follower
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + \
                                           self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')

        self.session_follow = session_follow   # Useful for convenience functions

        # Put data to the leader table
        value_prefix0 = '---'
        self.put_data(value_prefix0)
        self.session.checkpoint()

        # Check data in the follower
        self.disagg_advance_checkpoint(conn_follow)
        follower_cursor = self.session_follow.open_cursor(self.uri)
        self.check_data(follower_cursor, value_prefix0)
        follower_cursor.close()

        # Make a change on the leader, and propogate to the follower.
        value_prefix1 = '+++'
        self.put_data(value_prefix1)

        # Advance the checkpoint.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Step up. We have two connections, our old leader and the follower that is becoming
        # the new leader. Close the old leader first so there's no confusion within this test.
        self.conn.close()
        conn_follow.reconfigure('disaggregated=(role="leader")')

        # Now check that after closing, we get the new value
        follower_cursor = self.session_follow.open_cursor(self.uri)
        self.scan_data(follower_cursor, value_prefix1)
        follower_cursor.close()

        value_prefix2 = '!!!'
        self.put_data(value_prefix2, session = self.session_follow)

        # Now, the encryption part of the test. Remove all output up to now. We've previously
        # told PALM to be verbose and we're looking for a message that we've used the
        # encryption key. Doing a checkpoint should trigger the message.  Close down the connection
        # as well, as that generates other PALM verbose output that must be ignored.
        self.cleanStderr()
        self.cleanStdout()
        with self.expectedStdoutPattern('.*palm using saved dek.*', maxchars=10000):
            self.session_follow.checkpoint()
            session_follow.close()
            conn_follow.close()
