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
from wtscenario import make_scenarios

# test_prepare22.py
# Test rollback to stable clears the WT_UPDATE_TO_DELETE_HS flag.
class test_prepare22(wttest.WiredTigerTestCase):

    format_values = [
        ('column', dict(key_format='r', value_format='S')),
        ('column_fix', dict(key_format='r', value_format='8t')),
        ('row_integer', dict(key_format='i', value_format='S')),
    ]

    scenarios = make_scenarios(format_values)

    def test_prepare22(self):
        uri = "table:test_prepare22"
        self.session.create(uri, 'key_format=' + self.key_format + ',value_format=' + self.value_format)

        if self.value_format == '8t':
             value_a = 97
             value_b = 98
             value_c = 99
        else:
             value_a = "a"
             value_b = "b"
             value_c = "c"

        # Pin oldest timestamp to 1
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # Do the first update
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor[1] = value_a
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Do the second update
        self.session.begin_transaction()
        cursor[1] = value_b
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        # Do a prepared update
        self.session.begin_transaction()
        cursor[1] = value_c
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(30))

        # Evict the page
        session2 = self.conn.open_session()
        evict_cursor = session2.open_cursor(uri, None, "debug=(release_evict)")
        session2.begin_transaction("ignore_prepare=true")
        self.assertEquals(evict_cursor[1], value_b)
        evict_cursor.reset()
        evict_cursor.close()
        session2.rollback_transaction()

        # Ensure the history store is checkpointed
        session2.checkpoint()

        # Rollback the prepared transaction
        self.session.rollback_transaction()

        # Set stable timestamp to 20
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        # Call rollback to stable
        self.conn.rollback_to_stable()

        # Evict the page again
        evict_cursor = session2.open_cursor(uri, None, "debug=(release_evict)")
        session2.begin_transaction()
        self.assertEquals(evict_cursor[1], value_b)
        evict_cursor.reset()
        evict_cursor.close()
        session2.rollback_transaction()

        # Verify we can still read back value a
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        self.assertEquals(cursor[1], value_a)
        self.session.rollback_transaction()

        # Verify we can still read back value b
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        self.assertEquals(cursor[1], value_b)
        self.session.rollback_transaction()

if __name__ == '__main__':
    wttest.run()
