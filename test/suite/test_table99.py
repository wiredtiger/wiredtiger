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

# test_rollback_to_stable30.py
# Test RTS fails with active transactions and the subsequent transaction resolution succeeds.
class test_table99(wttest.WiredTigerTestCase):
    conn_config = "verbose=[recovery_progress],cache_size=30GB,statistics=(all),statistics_log=(json,on_close,wait=1)"
    uri = "table:many_table"

    def test_many_tables(self):
        create_params = 'value_format=S,key_format=S'

        for i in range (0, 10001):
            self.session.create(self.uri + str(i), create_params)
        
        self.session.begin_transaction()
        for i in range (0, 10001):
            cursor = self.session.open_cursor(self.uri + str(i))

            for j in range (0, 1000):
                cursor[str(j)] = "shrek" + str(j)

            cursor.close()

            if i % 1000 == 0:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
                self.session.checkpoint()
                self.session.begin_transaction()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()


        # Pin oldest and stable timestamps to 1.
        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(6) +
            ',stable_timestamp=' + self.timestamp_str(6))

        self.session.begin_transaction()
        for i in range (0, 10001):
            cursor = self.session.open_cursor(self.uri + str(i))

            for j in range (0, 1000):
                cursor[str(j)] = "octopus" + str(j)
            
            cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        simulate_crash_restart(self, ".", "RESTART")

if __name__ == '__main__':
    wttest.run()