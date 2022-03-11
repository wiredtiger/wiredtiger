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

import wiredtiger, wttest
from wtscenario import make_scenarios

# test_prepare21.py
# Verify we correctly look for an insert list prior to checking against an on-disk time window in
# row-store.
#
# The passing scenario is not returning a prepare conflict.
class test_prepare21(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=200MB,eviction=(threads_max=1)'

    def test_prepare_conflict_with_other_key_tw(self):
        uri = "table:test_prepare_conflict_with_other_key_tw"
        create_params = 'value_format=i,key_format=i'
        self.session.create(uri, create_params)
        cursor = self.session.open_cursor(uri)

        session2 = self.setUpSessionOpen(self.conn)
        session2.create(uri, create_params)
        cursor2 = session2.open_cursor(uri)

        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(1) + ',stable_timestamp=' + self.timestamp_str(1))

        # Insert a prepared value.
        self.session.begin_transaction()
        cursor[1] = 1
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(5))

        # Insert a value visible to the eviction cursor, cbt slots get a bit weird here so putting
        # this at key 6 instead of key 2 makes this test work.
        cursor2[6] = 2
        cursor2.reset()

        # Evict the prepared update and regular update.
        cursor.reset()
        evict_cursor = session2.open_cursor(uri, None, "debug=(release_evict)")
        evict_cursor.set_key(2)
        evict_cursor.search()
        evict_cursor.reset()

        # Insert then rollback another prepared value so we have an aborted prepared update on the
        # insert list.
        session2.begin_transaction()
        cursor2 = session2.open_cursor(uri)
        cursor2[3] = 3
        cursor2.reset()
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(6))
        session2.rollback_transaction()
        self.session.breakpoint()

        # Insert a value that will conflict with an on disk prepared update.
        session2.begin_transaction()
        cursor2 = session2.open_cursor(uri, None)
        cursor2[3] = 3

if __name__ == '__main__':
    wttest.run()
