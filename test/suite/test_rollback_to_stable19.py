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

import fnmatch, os, shutil, time
from helper import simulate_crash_restart
from test_rollback_to_stable01 import test_rollback_to_stable_base
from wiredtiger import stat, WT_NOTFOUND
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable19.py
# Test that rollback to stable aborts both insert and remove updates from a single prepared transaction
class test_rollback_to_stable19(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    in_memory_values = [
        ('no_inmem', dict(in_memory=False)),
        ('inmem', dict(in_memory=True))
    ]

    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    restart_options = [
        ('shutdown', dict(crash='false')),
        ('crash', dict(crash='true')),
    ]

    scenarios = make_scenarios(in_memory_values, key_format_values, restart_options)

    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all),log=(enabled=false),eviction_dirty_trigger=5,eviction_updates_trigger=5'
        if self.in_memory:
            config += ',in_memory=true'
        else:
            config += ',in_memory=false'
        return config

    def test_rollback_to_stable_no_history(self):
        nrows = 1000

        # Prepare transactions for column store table is not yet supported.
        if self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        uri = "table:rollback_to_stable19"
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        valuea = "aaaaa" * 100

        # Perform several updates and removes.
        s = self.conn.open_session()
        cursor = s.open_cursor(uri)
        s.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[ds.key(i)] = valuea
            cursor.set_key(i)
            cursor.remove()
        cursor.close()
        s.prepare_transaction('prepare_timestamp=' + timestamp_str(20))

        # Configure debug behavior on a cursor to evict the page positioned on when the reset API is used.
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search for the key so we position our cursor on the page that we want to evict.
        self.session.begin_transaction("ignore_prepare = true")
        evict_cursor.set_key(1)
        self.assertEquals(evict_cursor.search(), WT_NOTFOUND)
        evict_cursor.reset()
        evict_cursor.close()
        self.session.commit_transaction()

        # Search to make sure the data is not visible
        self.session.begin_transaction("ignore_prepare = true")
        cursor2 = self.session.open_cursor(uri)
        cursor2.set_key(1)
        self.assertEquals(cursor2.search(), WT_NOTFOUND)
        self.session.commit_transaction()

        # Pin stable timestamp to 20.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(20))
        if not self.in_memory:
            self.session.checkpoint()

        if not self.in_memory:
            if self.crash:
                simulate_crash_restart(self, ".", "RESTART")
            else:
                # Close and reopen the connection
                self.reopen_conn()
        else:
            self.conn.rollback_to_stable()
            s.rollback_transaction()

        # Verify data is not visible.
        self.check(valuea, uri, 0, 20)
        self.check(valuea, uri, 0, 30)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        keys_removed = stat_cursor[stat.conn.txn_rts_keys_removed][2]
        self.assertGreater(upd_aborted, 0)
        self.assertGreater(keys_removed, 0)

    def test_rollback_to_stable_with_history(self):
        nrows = 1000

        # Prepare transactions for column store table is not yet supported.
        if self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        uri = "table:rollback_to_stable19"
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        valuea = "aaaaa" * 100
        valueb = "bbbbb" * 100

        # Perform several updates.
        self.large_updates(uri, valuea, ds, nrows, 0, 20)

        # Perform several removes.
        self.large_removes(uri, ds, nrows, 0, 30)

        # Perform several updates and removes.
        s = self.conn.open_session()
        cursor = s.open_cursor(uri)
        s.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[ds.key(i)] = valueb
            cursor.set_key(i)
            cursor.remove()
        cursor.close()
        s.prepare_transaction('prepare_timestamp=' + timestamp_str(40))

        # Configure debug behavior on a cursor to evict the page positioned on when the reset API is used.
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")

        # Search for the key so we position our cursor on the page that we want to evict.
        self.session.begin_transaction("ignore_prepare = true")
        evict_cursor.set_key(1)
        self.assertEquals(evict_cursor.search(), WT_NOTFOUND)
        evict_cursor.reset()
        evict_cursor.close()
        self.session.commit_transaction()

        # Search to make sure the data is not visible
        self.session.begin_transaction("ignore_prepare = true")
        cursor2 = self.session.open_cursor(uri)
        cursor2.set_key(1)
        self.assertEquals(cursor2.search(), WT_NOTFOUND)
        self.session.commit_transaction()

        # Pin stable timestamp to 40.
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(40))
        if not self.in_memory:
            self.session.checkpoint()

        if not self.in_memory:
            if self.crash:
                simulate_crash_restart(self, ".", "RESTART")
            else:
                # Close and reopen the connection
                self.reopen_conn()
        else:
            self.conn.rollback_to_stable()
            s.rollback_transaction()

        # Verify data.
        self.check(valuea, uri, nrows, 20)
        self.check(valuea, uri, 0, 30)
        self.check(valuea, uri, 0, 40)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        self.assertGreater(upd_aborted, 0)
        if not self.in_memory:
            self.assertGreater(hs_removed, 0)
