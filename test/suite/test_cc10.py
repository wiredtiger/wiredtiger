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

import time
from test_cc01 import test_cc_base
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_cc10.py
# Test that checkpoint cleans the obsolete history store pages.
class test_cc10(test_cc_base):
    # Force a small cache.
    conn_config_common = 'cache_size=50MB,eviction_updates_trigger=95,eviction_updates_target=80,statistics=(all)'

    waiting_time = [
        ('1sec', dict(conn_config=f'{conn_config_common},checkpoint_cleanup=[wait=1]')),
        ('2sec', dict(conn_config=f'{conn_config_common},checkpoint_cleanup=[wait=2]')),
        ('3sec', dict(conn_config=f'{conn_config_common},checkpoint_cleanup=[wait=3]')),
    ]
    scenarios = make_scenarios(waiting_time)

    def test_cc10(self):
        nrows = 10000

        # Create a table without logging.
        uri = "table:cc10"
        ds = SimpleDataSet(self, uri, 0, key_format="i", value_format="S")
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))

        bigvalue = "aaaaa" * 100
        bigvalue2 = "ddddd" * 100
        self.large_updates(uri, bigvalue, ds, nrows, 10)

        # Check that all updates are seen.
        self.check(bigvalue, uri, nrows, 10)

        self.large_updates(uri, bigvalue2, ds, nrows, 100)

        # Check that the new updates are only seen after the update timestamp.
        self.check(bigvalue2, uri, nrows, 100)

        # Check that old updates are seen.
        self.check(bigvalue, uri, nrows, 10)

        # Pin oldest and stable to timestamp 100.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(100) +
            ',stable_timestamp=' + self.timestamp_str(100))

        # Wait for obsolete cleanup to occur, this should clean the history store.
        # We don't use the debug option to force cleanup as the thread should be running in the
        # background. Wait some time to make sure thread could run.
        time.sleep(5)
        self.check_cc_stats(force_cleanup=False)
