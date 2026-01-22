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

from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from rollback_to_stable_util import test_rollback_to_stable_base

# test_rollback_to_stable06.py
# Test that rollback to stable removes all keys when the stable timestamp is earlier than
# all commit timestamps.
class test_rollback_to_stable06(test_rollback_to_stable_base):

    format_values = [
        ('row_integer', dict(key_format='i', value_format='S')),
    ]

    value_format='S'

    in_memory_values = [
        ('inmem', dict(in_memory=True))
    ]

    prepare_values = [
        ('no_prepare', dict(prepare=False)),
    ]

    evict = [
        ('evict', dict(evict=True))
    ]

    worker_thread_values = [
        ('0', dict(threads=0)),
    ]

    scenarios = make_scenarios(format_values, in_memory_values, prepare_values, evict, worker_thread_values)
    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all),verbose=(rts:5)'
        if self.in_memory:
            config += ',in_memory=true'
        return config

    def test_rollback_to_stable(self):

        # Create a table.
        uri = "table:rollback_to_stable06"
        ds_config = ',log=(enabled=false)' if self.in_memory else ''
        ds = SimpleDataSet(self, uri, 0,
            key_format=self.key_format, value_format=self.value_format, config=ds_config)
        ds.populate()
        value_stable = "ssssss" * 100

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100
        value_d = "ddddd" * 100
        # Perform several updates.
        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))
        

        # insert a record at time 20
        session = self.session
        self.printOnce("Inserting record at key 100 at ts=20")
        cursor = session.open_cursor(uri)
        session.begin_transaction()

        for i in range(5000):
            cursor[ds.key(i)] = value_a
        
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        cursor.close()
        self.printOnce("Triggering eviction")
        self.session.breakpoint()
        self.evict_cursor(uri, 5000, None) # first nrows : save ts 20, last 5: save 40 
        # dekete the old record at time 30, and insert a new record at time 30
        # insert a record at time 20
        self.printOnce("Removing old record, Inserting record at key 200 at ts=20")
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        # for i in range(5000):
        #     cursor.set_key(ds.key(i))
        #     cursor.remove()
        for i in range(5000, 5200):
            cursor[ds.key(i)] = value_b

        session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Verify data is visible and correct.
        self.check(value_a, uri, 5000, 20)
        # self.check(value_b, uri, 200, 30) 

        # self.large_removes(uri, ds, nrows, self.prepare, 30) # tombstone in-memory
        # self.large_updates(uri, value_b, ds, nrows, self.prepare, 30) # insert in-memory
        self.session.breakpoint()
        self.printOnce("Triggering RTS") 
        self.conn.rollback_to_stable('threads=' + str(self.threads))

        self.printOnce("RTS done, nothing should be visible")  
        # Verify data is invisible.
        self.check(value_a, uri, 0, 30)
        # self.check(value_b, uri, 0, 50)


