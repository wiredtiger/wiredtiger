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
from wiredtiger import stat
from test_cc01 import test_cc_base
from helper import WiredTigerStat
from wtscenario import make_scenarios
import random
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# test_cc12.py
# Verify checkpoint cleanup cleans up logged tables when configured in aggressive mode.
@wttest.skip_for_hook("tiered", "Checkpoint cleanup does not support tiered tables")
class test_cc12(test_cc_base):
    conn_config_common = 'cache_size=8M,statistics=(all),statistics_log=(json,wait=1,on_close=true,sources=[file:])'

    checkpoint_cleanup_methods = [
        ('enable_log', dict(conn_config = conn_config_common+',log=(enabled=true)')),
        ('disable_log', dict(conn_config = conn_config_common+',log=(enabled=false)'))
    ]

    scenarios = make_scenarios(checkpoint_cleanup_methods)

    lock = threading.Lock()
    g_ts = 3
    opened_session = 1
    partitions = 100

    op_map = {}

    def random_populate(self, uri, seed, ts):
        tid = threading.get_ident()
        key_partition = 0
        with self.lock:
            key_partition = len(self.op_map)
            self.op_map[tid] = key_partition
        session = self.conn.open_session()
        try:
            for r in range(100):
                session.begin_transaction()
                cur = session.open_cursor(uri)
                for sr in range(10):
                    key = random.randint(0, 1000/self.partitions) * self.partitions + key_partition
                    cur[key] = f"upd_{r}_{sr}_{seed}"
                cur.close()
                with self.lock:
                    ts = self.g_ts
                    self.g_ts += 1
                session.commit_transaction(f'commit_timestamp=' + self.timestamp_str(ts))
            with self.lock:
                session.close()
                self.opened_session -= 1
        except Exception as e:
            session.close()
            print("Random populate failed : " + str(e))
        with self.lock:
            del self.op_map[tid]

    def long_transaction_round(self, uri, ts):
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(2))
        long_cur = self.session.open_cursor(uri)
        long_cur[-1] = "halted"
        long_cur.close()
        with ThreadPoolExecutor(max_workers=self.partitions) as executor:
            for seed in range(100):
                # self.random_populate(uri, seed, ts)
                executor.submit(self.random_populate, uri, seed, ts)
        self.session.commit_transaction()
        self.session.checkpoint('debug=(checkpoint_cleanup=true)')

    def get_hs_size(self):
        with WiredTigerStat(self.session) as stat_cursor:
            return stat_cursor[stat.conn.cache_hs_ondisk][2]

    def test_cc12(self):
        # Increase the likelihood of having internal pages since they are targeted by checkpoint
        # cleanup.
        create_params = 'key_format=i,value_format=S,allocation_size=512,internal_page_max=512,leaf_page_max=512'
        uri = 'table:cc12'

        self.session.create(uri, create_params)

        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        history_size = []

        populate_cur = self.session.open_cursor(uri)
        for key in range(10000):
            populate_cur[key] = "init"
        populate_cur.close()

        for ts in range(4):
            # with WiredTigerStat(self.session, 'history-store') as stat_cursor:
            #     history_size.append(stat_cursor[stat.dsrc.block_size][2])
            self.long_transaction_round(uri, ts)
            size_before = self.get_hs_size()
            self.wait_for_cc_to_run()
            history_size.append((size_before, self.get_hs_size()))

        self.prout("First rounds test: " + str(history_size))
        history_size.clear()

        for _ in range(2):
            self.reopen_conn()
            size_before = self.get_hs_size()
            self.wait_for_cc_to_run()
            history_size.append((size_before, self.get_hs_size()))
            size_before = self.get_hs_size()
            self.wait_for_cc_to_run()
            history_size.append((size_before, self.get_hs_size()))

        self.prout("Reopen rounds test: " + str(history_size))
        self.assertTrue(False, "Keep logs")
