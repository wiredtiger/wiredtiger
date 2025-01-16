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
# [TEST_TAGS]
# connection_api
# [END_TAGS]

import wttest, time
from threading import Thread

# test_timing_stress_for_test01.py
#   Test that the session_alter_slow connection configuration for timing_stress_for_test delays
#   the release of the schema lock in session alter.
class test_timing_stress_for_test01(wttest.WiredTigerTestCase):
    uri = 'table:timing_stress_for_test'
    conn_config = 'timing_stress_for_test=[session_alter_slow]'
    expected_delay = 2

    def alter_table(self):
        # Alter the table with any configuration.
        self.session.alter(self.uri, 'access_pattern_hint=random')

    def drop_table(self):
        start_time = time.time()

        # Start a second session.
        session2 = self.conn.open_session()
        # Drop the table in the second session.
        self.assertEqual(session2.drop(self.uri, None), 0)

        end_time = time.time()

        # Expect delay to be 2 seconds.
        self.assertGreaterEqual(end_time - start_time, self.expected_delay)

    def test_release_schema_lock_slow_no_wait(self):
        # Create a simple table in the first session.
        session1 = self.session
        session1.create(self.uri, 'key_format=S,value_format=S')

        # Create two threads to alter the table and drop the table.
        thread1 = Thread(target = self.alter_table)
        thread2 = Thread(target = self.drop_table)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()



