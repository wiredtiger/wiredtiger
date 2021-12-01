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
# test_cursor19.py
#   Test version cursor for modifies.
#
import wttest
import wiredtiger
from wtscenario import make_scenarios

WT_TS_MAX = 18446744073709551615

class test_cursor19(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor19'

    types = [
        ('row', dict(keyformat='i')),
        ('var', dict(keyformat='r'))
    ]

    scenarios = make_scenarios(types)

    def create(self):
        self.session.create(self.uri, 'key_format={},value_format=S'.format(self.keyformat))
    
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

    def test_modify(self):
        self.create()

        cursor = self.session.open_cursor(self.uri, None)
        # Add a value to the update chain
        self.session.begin_transaction()
        cursor[0] = "a"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(1))

        # Modify the value
        self.session.begin_transaction()
        mods = [wiredtiger.Modify("b", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))

        # Modify the value
        self.session.begin_transaction()
        mods = [wiredtiger.Modify("c", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))

        # Modify the value
        self.session.begin_transaction()
        mods = [wiredtiger.Modify("d", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(15))

        evict_cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict)")
        self.session.begin_transaction()
        self.assertEquals(evict_cursor[0], 0)
        evict_cursor.close()
        self.session.rollback_transaction()

        # Modify the value
        self.session.begin_transaction()
        mods = [wiredtiger.Modify("e", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(20))

        # Modify the value
        self.session.begin_transaction()
        mods = [wiredtiger.Modify("f", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(25))

        # Open a version cursor
        version_cursor = self.session.open_cursor(self.uri, "debug=(dump_version=true)")
        version_cursor.set_key(0)
        self.assertEquals(version_cursor.search(), 0)
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 25, 25, WT_TS_MAX, WT_TS_MAX, 2, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), "f")
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 20, 20, 25, 25, 2, 0, 0, 0)
        self.assertEquals(version_cursor.get_value(), "e")
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 15, 15, 20, 20, 3, 0, 0, 1)
        self.assertEquals(version_cursor.get_value(), "d")
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 10, 10, 15, 15, 3, 0, 0, 2)
        self.assertEquals(version_cursor.get_value(), "c")
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 5, 5, 10, 10, 2, 0, 0, 3)
        self.assertEquals(version_cursor.get_value(), "b")
        self.assertEquals(version_cursor.next(), 0)
        self.verify_keys(version_cursor, 1, 1, 5, 5, 2, 0, 0, 3)
        self.assertEquals(version_cursor.get_value(), "a")
        self.assertEquals(version_cursor.next(), wiredtiger.WT_NOTFOUND)
