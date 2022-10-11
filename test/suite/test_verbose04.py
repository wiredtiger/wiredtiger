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

from helper import simulate_crash_restart
from test_verbose01 import test_verbose_base
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wtthread import checkpoint_thread
import re, threading
import wiredtiger, wttest

def mod_val(value, char, location, nbytes=1):
    return value[0:location] + char + value[location+nbytes:]

# test_verbose04.py
# Verify extended debug verbose levels (WT_VERBOSE_DEBUG_2 through 5).
class test_verbose04(test_verbose_base):
    def updates(self, uri, value, ds, nrows, commit_ts):
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def test_verbose_level_2(self):
        self.close_conn()

        with self.expect_verbose(['rts:5'], ['DEBUG_1', 'DEBUG_2'], False) as conn:
            self.conn = conn
            self.session = self.conn.open_session()

            uri = "table:test_verbose04"
            create_params = 'key_format=i,value_format=S'
            self.session.create(uri, create_params)

            ds = SimpleDataSet(self, uri, 0, key_format='i', value_format="S")
            ds.populate()

            nrows = 1000
            value = 'x' * 1000

            # Insert values with varying timestamps.
            self.updates(uri, value, ds, nrows, 20)

            # Move the oldest and stable timestamps to 40.
            self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(40) +
                                    ', stable_timestamp=' + self.timestamp_str(40))

            # Update values.
            self.updates(uri, value, ds, nrows, 60)

            # Perform a checkpoint.
            self.session.checkpoint('use_timestamp=true')

    # test_rollback_to_stable06,14, and 28
    def test_verbose_level_3(self):
        pass

    def test_verbose_level_4(self):
        # test_compat01.test_reconfig looks good here
        # test_checkpoint_snapshot02
        # test_hs06.test_hs_reads
        # test_truncate10
        pass

    def test_verbose_level_5(self):
        ckpt_uri = 'table:ckpt_table'
        # logged_uri = 'table:logged_table'
        #
        # Create a collection-like table that is checkpoint durability (that is, logging has been
        # turned off), and an oplog-like table that is commit-level durability. Add data to each
        # of them separately and checkpoint so each one has a different stable timestamp.
        #
        basecfg = 'key_format=i,value_format=i'
        # self.session.create(logged_uri, basecfg)
        self.session.create(ckpt_uri, basecfg + ',log=(enabled=false)')
        # c_logged = self.session.open_cursor(logged_uri)
        c_ckpt = self.session.open_cursor(ckpt_uri)

        # Begin by adding some data.
        nentries = 10
        first_range = range(1, nentries)
        second_range = range(nentries, nentries*2)
        all_keys = range(1, nentries*2)
        for i in first_range:
            self.session.begin_transaction()
            # c_logged[i] = 1
            c_ckpt[i] = 1
            self.session.commit_transaction(
              'commit_timestamp=' + self.timestamp_str(i))
        # Set the oldest and stable timestamp to the end.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(nentries-1) +
        ',stable_timestamp=' + self.timestamp_str(nentries-1))

        # Add more data but don't advance the stable timestamp.
        for i in second_range:
            self.session.begin_transaction()
            # c_logged[i] = 1
            c_ckpt[i] = 1
            self.pr("i: " + str(i))
            self.session.commit_transaction(
              'commit_timestamp=' + self.timestamp_str(i))

        # Close and reopen the connection. We cannot use reopen_conn because
        # we want to test the specific close configuration string.
        self.close_conn()
        self.open_conn()

        # Set up our expected data and verify after the reopen.
        # logged_exp = dict((k, 1) for k in all_keys)
        # ckpt_exp = dict((k, 1) for k in first_range)

        # self.verify_expected(logged_exp, ckpt_exp)

    # def test_verbose_level_5(self):
    #     conn_config = 'config_base=false,create,log=(enabled)'
    #     ckpt_uri = 'table:ckpt_table'
    #     logged_uri = 'table:logged_table'

    #     #
    #     # Create a collection-like table that is checkpoint durability (that is, logging has been
    #     # turned off), and an oplog-like table that is commit-level durability. Add data to each
    #     # of them separately and checkpoint so each one has a different stable timestamp.
    #     #
    #     basecfg = 'key_format=i,value_format=i'
    #     self.session.create(logged_uri, basecfg)
    #     self.session.create(ckpt_uri, basecfg + ',log=(enabled=false)')
    #     c_logged = self.session.open_cursor(logged_uri)
    #     c_ckpt = self.session.open_cursor(ckpt_uri)

    #     # Begin by adding some data.
    #     nentries = 10
    #     first_range = range(1, nentries)
    #     second_range = range(nentries, nentries*2)
    #     all_keys = range(1, nentries*2)
    #     for i in first_range:
    #         self.session.begin_transaction()
    #         c_logged[i] = 1
    #         c_ckpt[i] = 1
    #         self.session.commit_transaction(
    #           'commit_timestamp=' + self.timestamp_str(i))
    #     # Set the oldest and stable timestamp to the end.
    #     self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(nentries-1) +
    #     ',stable_timestamp=' + self.timestamp_str(nentries-1))

    #     # Add more data but don't advance the stable timestamp.
    #     for i in second_range:
    #         self.session.begin_transaction()
    #         c_logged[i] = 1
    #         c_ckpt[i] = 1
    #         self.pr("i: " + str(i))
    #         self.session.commit_transaction(
    #           'commit_timestamp=' + self.timestamp_str(i))

    #     # Close and reopen the connection. We cannot use reopen_conn because
    #     # we want to test the specific close configuration string.
    #     self.close_conn()
    #     self.open_conn()

if __name__ == '__main__':
    wttest.run()
