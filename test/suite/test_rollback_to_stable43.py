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
from helper import simulate_crash_restart
from rollback_to_stable_util import test_rollback_to_stable_base
from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_rollback_to_stable43.py
# Test the rollback to stable should retain/restore the tombstone from
# the update list or from the history store for on-disk database.
class test_rollback_to_stable43(test_rollback_to_stable_base):

    format_values = [
        # ('column', dict(key_format='r', value_format='S')),
        # ('column_fix', dict(key_format='r', value_format='8t')),
        ('row_integer', dict(key_format='i', value_format='S')),
    ]

    prepare_values = [
        ('no_prepare', dict(prepare=False)),
        # ('prepare', dict(prepare=True))
    ]

    dryrun_values = [
        ('no_dryrun', dict(dryrun=False)),
        # ('dryrun', dict(dryrun=True)),
    ]

    scenarios = make_scenarios(format_values, prepare_values, dryrun_values)

    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all),verbose=(rts:5),debug_mode=(update_restore_evict=true)'
        return config

    def test_tmp(self):
        nrows = 1
        uri = "table:rollback_to_stable43"
        self.session.create(uri, 'key_format=i,value_format=S')
        value = "abcdef" * 3
        value1a = "defghi" * 3
        value2 = "ghijkl" * 3
        value3 = "mnopqr" * 3

        # insert, make this stable
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            cursor[i] = value
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        self.conn.set_timestamp('oldest_timestamp={},stable_timestamp={}'.format(self.timestamp_str(10), self.timestamp_str(20)))

        # delete, unstable
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            cursor.set_key(i)
            cursor.remove()
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(22))

        # eviction
        self.session.begin_transaction()
        evict_cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        for i in range(1, nrows + 1):
            evict_cursor.set_key(i)
            evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(40))

        # normal update, unstable, uncommitted
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            cursor[i] = value3
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))

        self.session.checkpoint()

        # RTS
        self.conn.rollback_to_stable()

        # another delete, committed
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            cursor.set_key(i)
            cursor.remove()
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(60))

        # eviction
        self.session.begin_transaction()
        evict_cursor = self.session.open_cursor(uri, None, 'debug=(release_evict)')
        for i in range(1, nrows + 1):
            evict_cursor.set_key(i)
            evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(70))
