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

import os, wiredtiger, wttest
import threading
from wtbackup import backup_base
from wtscenario import make_scenarios
import random
from concurrent.futures import ThreadPoolExecutor
import shutil
import time

# test_backup31.py
# Test querying the checkpoint timestamp for a backup cursor.
class test_backup31(backup_base):
    conn_config = 'cache_size=8M,statistics=(fast),checkpoint=(wait=0),log=(enabled=false)'

    dir='backup.dir'                    # Backup directory name
    uri="table:table31"
    
    lock = threading.Lock()
    g_ts = 3
    opened_session = 1
    partitions = 100

    op_map = {}

    def random_populate(self, uri, seed):
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
    
    def operation_on_markers(self, uri, start_key, end_key, op_type):
        session = self.conn.open_session()
        c = session.open_cursor(uri)
        session.begin_transaction()
        self.g_ts += 1
        ts = self.g_ts
        for key in range(start_key, end_key):
            if op_type == 'update':
                c[key] = f"val_{ts}"
            elif op_type == 'remove':
                c.set_key(key)
                c.remove()
        session.commit_transaction(f'commit_timestamp=' + self.timestamp_str(ts))
        c.close()
        session.close()

    def long_transaction_round(self, uri):
        self.operation_on_markers(uri, -200, -100, 'update')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(self.g_ts))
        self.g_ts += 1
        long_cur = self.session.open_cursor(uri)
        with ThreadPoolExecutor(max_workers=self.partitions) as executor:
            for seed in range(50):
                # self.random_populate(uri, seed, ts)
                executor.submit(self.random_populate, uri, seed)
        self.operation_on_markers(uri, -200, -100, 'remove')
        long_cur[-1] = long_cur[-150] 
        long_cur.close()
        self.g_ts += 1
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.g_ts))
        
        with ThreadPoolExecutor(max_workers=self.partitions) as executor:
            for seed in range(100):
                # self.random_populate(uri, seed, ts)
                executor.submit(self.random_populate, uri, seed)
        self.session.commit_transaction()
        # Here we got a short window of removed key
        
    def test_backup31(self):
        self.session.create(self.uri, "key_format=i,value_format=S")
        self.long_transaction_round(self.uri)
        """"""
        # Open a backup cursor.
        bkup_c = self.session.open_cursor('backup:', None, None)
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.mkdir(self.dir)
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            shutil.copy(bkup_c.get_key(), self.dir)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()

        self.close_conn('use_timestamp=false')
        
        backup_conn = wiredtiger.wiredtiger_open(self.dir, 'create')
        oldest = backup_conn.query_timestamp('get=oldest')
        stable = backup_conn.query_timestamp('get=stable')
        print('backup oldest ts =', oldest, 'stable ts =', stable)
        backup_session = backup_conn.open_session()
        for _ in range(5):
            backup_session.verify(self.uri)
            time.sleep(1)
        backup_conn.close()