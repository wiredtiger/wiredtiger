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
# test_version_search_near.py
#   Test WT_CURSOR::search_near for the version cursor.
#
import wttest
import wiredtiger

# The version cursor value is the metadata format (14 fields) followed by the table value, so the
# table value lives at index 14 of get_values().
VALUE_INDEX = 14

class test_version_search_near(wttest.WiredTigerTestCase):
    uri = 'file:test_version_search_near.wt'

    def create(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

    def key(self, i):
        # Zero-padded so lexicographic order matches numeric order.
        return 'key%03d' % i

    def open_version_cursor(self, start_timestamp=None):
        internal_config = ["enabled=true"]
        if start_timestamp is not None:
            internal_config.append("start_timestamp=" + self.timestamp_str(start_timestamp))
        config = "debug=(dump_version=(" + ",".join(internal_config) + "))"
        return self.session.open_cursor(self.uri, None, config)

    def populate(self, keys, commit_ts=10):
        """Insert one committed version per key at the given commit timestamp."""
        cursor = self.session.open_cursor(self.uri, None)
        for i in keys:
            self.session.begin_transaction()
            cursor[self.key(i)] = 'value%03d' % i
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(commit_ts))
        cursor.close()

    def get_value(self, version_cursor):
        return version_cursor.get_values()[VALUE_INDEX]

    def test_exact_match(self):
        """An exact match returns 0 and positions on the searched key."""
        self.create()
        self.populate([10, 20, 30])

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(20))
        self.assertEqual(vc.search_near(), 0)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020')
        vc.close()
        self.session.rollback_transaction()

    def test_between_keys(self):
        """A search key between two existing keys lands on the next greater key, exact > 0."""
        self.create()
        self.populate([10, 20, 30])

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(15))
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020')
        vc.close()
        self.session.rollback_transaction()

    def test_smaller_than_all(self):
        """A search key smaller than all keys lands on the smallest key, exact > 0."""
        self.create()
        self.populate([10, 20, 30])

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(5))
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(10))
        self.assertEqual(self.get_value(vc), 'value010')
        vc.close()
        self.session.rollback_transaction()

    def test_larger_than_all(self):
        """A search key larger than all keys lands on the largest key, exact < 0."""
        self.create()
        self.populate([10, 20, 30])

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(99))
        self.assertEqual(vc.search_near(), -1)
        self.assertEqual(vc.get_key(), self.key(30))
        self.assertEqual(self.get_value(vc), 'value030')
        vc.close()
        self.session.rollback_transaction()

    def test_next_after_search_near(self):
        """After a successful search_near, next() iterates forward over versions and keys."""
        self.create()
        # key020 gets two versions; the search_near should land on the newest one.
        cursor = self.session.open_cursor(self.uri, None)
        for i in [10, 20, 30]:
            self.session.begin_transaction()
            cursor[self.key(i)] = 'value%03d' % i
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))
        self.session.begin_transaction()
        cursor[self.key(20)] = 'value020b'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(20))
        cursor.close()

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(15))
        # Lands on key020, newest version first.
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020b')

        # next() walks back through the older version of the same key.
        self.assertEqual(vc.next(), 0)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020')

        # No more versions of key020; next() exhausts the single key (cross_key is off).
        self.assertEqual(vc.next(), wiredtiger.WT_NOTFOUND)
        vc.close()
        self.session.rollback_transaction()

    def test_empty_table(self):
        """An empty table returns WT_NOTFOUND."""
        self.create()

        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(10))
        self.assertEqual(vc.search_near(), wiredtiger.WT_NOTFOUND)
        vc.close()
        self.session.rollback_transaction()

    def test_all_versions_filtered(self):
        """
        A key whose only version is filtered out by start_timestamp is skipped; search_near lands
        on the next visible key.
        """
        self.create()
        # key010 committed early (durable ts 5), key020 committed later (durable ts 20).
        cursor = self.session.open_cursor(self.uri, None)
        self.session.begin_transaction()
        cursor[self.key(10)] = 'value010'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))
        self.session.begin_transaction()
        cursor[self.key(20)] = 'value020'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(20))
        cursor.close()

        # With start_timestamp=10, key010's version (durable ts 5) is filtered out.
        self.session.begin_transaction()
        vc = self.open_version_cursor(start_timestamp=10)
        vc.set_key(self.key(10))
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020')
        vc.close()
        self.session.rollback_transaction()

    def test_all_versions_filtered_backward(self):
        """
        When the only visible key is below the search key (everything at/after is filtered),
        search_near falls back to the largest preceding visible key, exact < 0.
        """
        self.create()
        # key010 committed later (durable ts 20, visible), key020 committed early (durable ts 5).
        cursor = self.session.open_cursor(self.uri, None)
        self.session.begin_transaction()
        cursor[self.key(10)] = 'value010'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(20))
        self.session.begin_transaction()
        cursor[self.key(20)] = 'value020'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))
        cursor.close()

        # Search for key020 with start_timestamp=10: key020 (durable ts 5) is filtered, the only
        # visible key (key010) is below the search key.
        self.session.begin_transaction()
        vc = self.open_version_cursor(start_timestamp=10)
        vc.set_key(self.key(20))
        self.assertEqual(vc.search_near(), -1)
        self.assertEqual(vc.get_key(), self.key(10))
        self.assertEqual(self.get_value(vc), 'value010')
        vc.close()
        self.session.rollback_transaction()

    def test_deleted_key_with_history(self):
        """
        A key whose newest committed version is a tombstone (so it reads as absent) still has older
        versions, and the version cursor must position on it rather than skipping past to the next
        live key.
        """
        self.create()
        cursor = self.session.open_cursor(self.uri, None)
        # key010 gets a value then a tombstone, both committed before the reader's snapshot.
        self.session.begin_transaction()
        cursor[self.key(10)] = 'value010'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))
        self.session.begin_transaction()
        cursor.set_key(self.key(10))
        cursor.remove()
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))
        # key020 is a live value.
        self.session.begin_transaction()
        cursor[self.key(20)] = 'value020'
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(5))
        cursor.close()

        # Read after the tombstone so key010 appears deleted to an ordinary cursor.
        self.session.begin_transaction("read_timestamp=" + self.timestamp_str(20))
        vc = self.open_version_cursor()
        vc.set_key(self.key(5))
        # The smallest key at or after key005 with a visible version is key010 (its older value).
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(10))

        # Walking forward then reaches key020, the live key.
        vc.reset()
        vc.set_key(self.key(15))
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), self.key(20))
        self.assertEqual(self.get_value(vc), 'value020')
        vc.close()
        self.session.rollback_transaction()

    def test_column_store(self):
        """
        Repeat the core cases on a column-store table, where search_near positions by record number
        rather than by key bytes.
        """
        col_uri = 'file:test_version_search_near_col.wt'
        self.session.create(col_uri, 'key_format=r,value_format=S')
        cursor = self.session.open_cursor(col_uri, None)
        for r in [10, 20, 30]:
            self.session.begin_transaction()
            cursor[r] = 'value%03d' % r
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))
        cursor.close()

        self.session.begin_transaction()
        vc = self.session.open_cursor(col_uri, None, "debug=(dump_version=(enabled=true))")

        # Exact match.
        vc.set_key(20)
        self.assertEqual(vc.search_near(), 0)
        self.assertEqual(vc.get_key(), 20)

        # Between two records: lands on the next greater.
        vc.reset()
        vc.set_key(15)
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), 20)

        # Smaller than all records.
        vc.reset()
        vc.set_key(5)
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), 10)

        # Larger than all records.
        vc.reset()
        vc.set_key(99)
        self.assertEqual(vc.search_near(), -1)
        self.assertEqual(vc.get_key(), 30)

        # Iterate forward after landing.
        vc.reset()
        vc.set_key(15)
        self.assertEqual(vc.search_near(), 1)
        self.assertEqual(vc.get_key(), 20)
        self.assertEqual(vc.next(), wiredtiger.WT_NOTFOUND)
        vc.close()
        self.session.rollback_transaction()

    def test_prepared_key_no_conflict(self):
        """
        search_near must not raise WT_PREPARE_CONFLICT when a key at/near the search key has only a
        prepared (uncommitted) value. The anchor search ignores prepare; the key-only walk then
        surfaces or skips the key per the reader's visibility.
        """
        self.create()
        self.populate([10, 30])  # committed key010, key030

        # A second session leaves key020 prepared (uncommitted) at prepare_timestamp 5.
        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.uri, None)
        session2.begin_transaction()
        cursor2[self.key(20)] = 'value020'
        session2.prepare_transaction("prepare_timestamp=" + self.timestamp_str(5))

        # A plain snapshot reader (NOT ignore_prepare) searches across the prepared key. Without the
        # anchor's ignore-prepare this raises WT_PREPARE_CONFLICT; with it the call must succeed.
        self.session.begin_transaction()
        vc = self.open_version_cursor()
        vc.set_key(self.key(20))
        # The call must succeed (no WT_PREPARE_CONFLICT). The version cursor surfaces the prepared
        # version, so the exact key is found.
        self.assertEqual(vc.search_near(), 0)
        self.assertEqual(vc.get_key(), self.key(20))
        vc.close()
        self.session.rollback_transaction()
        session2.rollback_transaction()

if __name__ == '__main__':
    wttest.run()
