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
# test_cursor18.py
#   Test version cursor under various scenarios.
#
import wttest
import wiredtiger
from wtscenario import make_scenarios

WT_TS_MAX = 18446744073709551615

class test_cursor18(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor18'

    types = [
        ('row', dict(keyformat='i', valueformat='i')),
        ('var', dict(keyformat='r', valueformat='i')),
        ('fix', dict(keyformat='r', valueformat='8t')),
    ]

    scenarios = make_scenarios(types)

    def create(self):
        self.session.create(self.uri, 'key_format={},value_format={}'.format(self.keyformat, self.valueformat))
    
    def verify_keys(self, version_cursor, expected_start_ts, expected_start_durable_ts, expected_stop_ts, expected_stop_durable_ts, expected_type, expected_prepare_state, expected_flags, expected_location):
        [_, start_ts, start_durable_ts, _, stop_ts, stop_durable_ts, type, prepare_state, flags, location] = version_cursor.get_keys()
        self.assertEquals(start_ts, expected_start_ts)
        self.assertEquals(start_durable_ts, expected_start_durable_ts)
        self.assertEquals(stop_ts, expected_stop_ts)
        self.assertEquals(stop_durable_ts, expected_stop_durable_ts)
        self.assertEquals(type, expected_type)
        self.assertEquals(prepare_state, expected_prepare_state)
        self.assertEquals(flags, expected_flags)
        self.assertEquals(location, expected_location)

    def test_update_chain_only(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        # Update the value
        self.session.bigin_transaction()
        cursor[0] = 1
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 5, 5, WT_TS_MAX, WT_TS_MAX, 3, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), 1)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 3, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_ondisk_only(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, WT_TS_MAX, WT_TS_MAX, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_ondisk_only_with_deletion(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        # Delete the value
        self.session.bigin_transaction()
        cursor.set_key(0)
        self.assertEquals(cursor.remove(), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_ondisk_with_deletion_on_update_chain(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Delete the value
        self.session.bigin_transaction()
        cursor.set_key(0)
        self.assertEquals(cursor.remove(), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_ondisk_with_hs(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        # Update the value
        self.session.bigin_transaction()
        cursor[0] = 1
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 5, 5, WT_TS_MAX, WT_TS_MAX, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), 1)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 3, 0, 0, 2)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_update_chain_ondisk_hs(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.bigin_transaction()
        cursor[0] = 0
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        # Update the value
        self.session.bigin_transaction()
        cursor[0] = 1
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Update the value
        self.session.bigin_transaction()
        cursor[0] = 2
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 10, 10, WT_TS_MAX, WT_TS_MAX, 3, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), 2)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 5, 5, 10, 10, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), 1)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 3, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)

    def test_prepare(self):
        self.create()

        session2 = self.conn.open_session()
        cursor = session2.open_cursor(self.uri, None)
        # Add a value to the update chain
        session2.bigin_transaction()
        cursor[0] = 0
        session2.prepare_transaction("prepare_timestamp=" + self.timestamp_str(1))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        evict_cursor.set_key(0)
        self.assertEquals(evict_cursor.search(), wiredtiger.WT_PREPARE_CONFLICT)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(version_cursor=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, WT_TS_MAX, WT_TS_MAX, 3, 1, 0x40, 0)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 1, 1, 3, 1, 0, 1)
        self.assertEquals(version_cursor.get_value(), 0)
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)
