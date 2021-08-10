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
import wiredtiger, wttest
from wtscenario import make_scenarios

# test_rollback_to_stable26.py
# Make sure that large key-space gaps don't cause a performance problem
class test_rollback_to_stable26(wttest.WiredTigerTestCase):
    session_config = 'isolation=snapshot'
    conn_config = 'in_memory=false'

    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]
    btime_values = [
        ('none', dict(btime=None)),
        ('before', dict(btime=25)),
        ('after', dict(btime=75)),
    ]
    evicttime_values = [
        ('none', dict(evicttime=None)),
        ('mid', dict(evicttime=50)),
        ('late', dict(evicttime=75)),
    ]

    scenarios = make_scenarios(key_format_values, btime_values, evicttime_values)

    def evict(self, s, uri, key, value):
        # Evict the page to force reconciliation.
        evict_cursor = s.open_cursor(uri, None, "debug=(release_evict)")
        s.begin_transaction()
        # Search the key to evict it.
        v = evict_cursor[key]
        self.assertEqual(v, value)
        self.assertEqual(evict_cursor.reset(), 0)
        s.rollback_transaction()
        evict_cursor.close()

    def test_rollback_to_stable26(self):
        # Create a table without logging.
        uri = "table:rollback_to_stable26"
        format = 'key_format={},value_format=S'.format(self.key_format)
        self.session.create(uri, format + ', log=(enabled=false)')

        # Pin oldest timestamp to 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))

        # Start stable timestamp at 10.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        # I've chosen key_b such that if the test fails it takes about fifteen seconds on an
        # old machine. This gives a margin against faster machines accidentally accepting the
        # failing behavior without taking excessively long if the failure behavior does appear.
        # Increase if necessary in the future.
        key_a = 1
        #key_b = 500000000
        key_b = 80000000
        key_c = key_b * 2

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100

        s = self.conn.open_session()
        cursor = s.open_cursor(uri)

        # If requested, write key_b at time 25.
        if self.btime == 25:
             s.begin_transaction()
             cursor[key_b] = value_b
             s.commit_transaction('commit_timestamp=' + self.timestamp_str(25))

        # Write the other keys at time 50.
        s.begin_transaction()
        cursor[key_a] = value_a
        cursor[key_c] = value_c
        s.commit_transaction('commit_timestamp=' + self.timestamp_str(50))

        # If requested, evict now.
        if self.evicttime == 50:
            self.evict(s, uri, key_a, value_a)

        # If requested, write key_b at time 75.
        if self.btime == 75:
             s.begin_transaction()
             cursor[key_b] = value_b
             s.commit_transaction('commit_timestamp=' + self.timestamp_str(75))

        # If requested, evict now.
        if self.evicttime == 75:
            self.evict(s, uri, key_a, value_a)

        cursor.close()

        # Now roll back to 60. Allow it one second. It should take far less than that. If it
        # tries to process the intermediate keys (implausible for rows, used to happen in some
        # cases for columns) it'll take considerably longer. Python doesn't guarantee that the
        # timer resolution is any better than seconds, so we have to accept "1" second as a
        # possible manifestation of a much shorter time.
        #
        # Note that while it would be better to have the test time out instead of waiting for
        # the operation to finish, there doesn't appear to be any good way to do that without
        # using Unix interfaces and making the test non-portable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(60))
        start = time.time()
        self.conn.rollback_to_stable()
        end = time.time()
        if end - start > 1:
            self.fail("Rollback to stable ran for far too long")
