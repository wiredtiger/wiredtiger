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

from time import sleep
import wttest, threading, wiredtiger
from helper import simulate_crash_restart
from wtscenario import make_scenarios

class test_evict_race01(wttest.WiredTigerTestCase):
    conn_config = 'timing_stress_for_test=(checkpoint_slow)'
    session_config = 'isolation=snapshot'
    uri = 'table:evict_race01'
    numrows = 1

    key_format_values = [
        # ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    scenarios = make_scenarios(key_format_values)

    value1 = 'aaaaa'
    value2 = 'bbbbb'
    value3 = 'ccccc'
    value4 = 'ddddd'

    def test_out_of_order_ts(self):
        self.session.create(self.uri, 'key_format={},value_format=S'.format(self.key_format))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        cursor = self.session.open_cursor(self.uri)
        # Insert a value at timestamp 4
        self.session.begin_transaction()
        cursor[1] = self.value1
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(4))

        # Insert a value at timetamp 5
        self.session.begin_transaction()
        cursor[1] = self.value2
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))

        # Move the stable timestamp.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(4))

        # Insert a value at timetamp 6.
        self.session.begin_transaction()
        cursor[1] = self.value3
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(6))
        cursor.close()

        # Create two threads, one for eviciton and one for OOO timestamps.
        ooo_thread = threading.Thread(target=self.out_of_order_ts_commits)
        evict_thread = threading.Thread(target=self.evict_data)


        self.session.breakpoint()

        # Start both threads.
        ooo_thread.start()
        # Comment out to fix test.
        #evict_thread.start()
        self.session.checkpoint()
        # Comment out to fix test.
        #evict_thread.join()
        ooo_thread.join()
        simulate_crash_restart(self, '.', "RESTART")
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(4))
        self.assertEquals(value1, cursor[1])
        self.session.rollback_transaction()

    def evict_data(self):
        sleep(2)
        session = self.setUpSessionOpen(self.conn)
        evict_cursor = session.open_cursor(self.uri, None, "debug=(release_evict)")
        evict_cursor.set_key(1)
        self.assertEquals(evict_cursor.search(), 0)
        evict_cursor.reset()
        evict_cursor.close()
        session.close()

    def out_of_order_ts_commits(self):
        sleep(0.5)
        session = self.setUpSessionOpen(self.conn)
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        cursor[1] = self.value4
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(4))
        cursor.close()
        session.close()