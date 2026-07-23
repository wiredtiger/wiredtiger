#!/usr/bin/env python3
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

# test_layered_tombstone_value.py
#   A layered tombstone is two 0x14 bytes. Application values that begin with that sequence share
#   the encoded namespace and are escaped before being written to the stable table. This
#   test confirms that such values are counted by category as they cross the stable read and write
#   paths on a disaggregated leader.
#
#   The statistics count the stored (encoded) form. Encoding appends a single tombstone byte to any
#   value in the namespace, so the public write path can only produce two of the four shapes: 'three
#   tombstone bytes', and a value that ends with the appended tombstone byte. The other two buckets
#   (the bare tombstone and a value ending in a non-tombstone byte) can only arise from legacy
#   unescaped data on disk, so this test cannot produce them.

import wttest
from wiredtiger import Modify, stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_tombstone_value(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Application value -> stored (encoded) form -> category it is counted under.
    normal = b'hello'         # unchanged                 -> not in the namespace
    tomb = b'\x14\x14'        # \x14\x14\x14              -> three tombstone bytes
    triple = b'\x14\x14\x14'  # \x14\x14\x14\x14          -> suffix (trailing tombstone byte)
    mixed = b'\x14\x14ab'     # \x14\x14ab\x14            -> suffix (trailing appended byte)

    # Application value -> modify operation -> resulting application value.
    # The first two results enter the namespace; the last result leaves it.
    modify_cases = (
        (b'ab', [Modify(b'\x14\x14', 0, 2)], b'\x14\x14'),      # \x14\x14\x14
        (b'abcd', [Modify(b'\x14\x14', 0, 2)], b'\x14\x14cd'),  # \x14\x14cd\x14
        (b'\x14\x14ab', [Modify(b'a', 0, 1)], b'a\x14ab'),      # a\x14ab, not in the namespace
    )

    def conn_config(self):
        return self.extensionsConfig() + \
            ',create,statistics=(all),disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        # Encountering these values is the behavior under test, so the warnings are expected.
        self.ignoreStdoutPattern('stable table value in the tombstone namespace')
        self.session.create(self.uri, 'key_format=i,value_format=u')

    def get_stat(self, which):
        stat_cursor = self.session.open_cursor('statistics:')
        value = stat_cursor[which][2]
        stat_cursor.close()
        return value

    # Counts of (tombstone, three-byte, suffix, prefix) values seen on the stable path.
    def category_counts(self):
        return (
            self.get_stat(stat.conn.layered_curs_stable_value_tombstone),
            self.get_stat(stat.conn.layered_curs_stable_value_tombstone_x3),
            self.get_stat(stat.conn.layered_curs_stable_value_tombstone_suffix),
            self.get_stat(stat.conn.layered_curs_stable_value_tombstone_prefix),
        )

    def test_stable_value_categories(self):
        items = {1: self.normal, 2: self.tomb, 3: self.triple, 4: self.mixed}

        # Write on the leader (write path). Each namespace value gains a trailing tombstone byte;
        # the bare tombstone becomes three such bytes.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for key, value in items.items():
            cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        self.assertEqual(self.category_counts(), (0, 1, 2, 0))

        # Read the values back from the stable table (read path); each category is counted again.
        # The stored value is decoded on the way out, so the application sees the original bytes.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        for key, value in items.items():
            cursor.set_key(key)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), value)
        self.session.rollback_transaction()

        self.assertEqual(self.category_counts(), (0, 2, 4, 0))

    def test_verify_counts_stable_values(self):
        # verify() walks the stable table's pages directly, bypassing the layered cursor, so the
        # check is pushed down to the page walk. Seed the values, persist them with a checkpoint,
        # then confirm verify accounts for each stored value.
        items = {1: self.normal, 2: self.tomb, 3: self.triple, 4: self.mixed}
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for key, value in items.items():
            cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # Measure only what verify adds. The stored forms are one three-tombstone-byte encoding and
        # two suffix-form (trailing 0x14) encodings; the normal value is not counted.
        before = self.category_counts()
        self.session.verify(self.uri)
        after = self.category_counts()
        self.assertEqual(tuple(a - b for a, b in zip(after, before)), (0, 1, 2, 0))

    def check_modify_namespace_transitions(self, cursor):
        self.session.begin_transaction()
        for key, (value, _mods, _result) in enumerate(self.modify_cases, 1):
            cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Exercise modify operations that enter or leave the tombstone namespace.
        self.session.begin_transaction()
        for key, (_value, mods, _result) in enumerate(self.modify_cases, 1):
            cursor.set_key(key)
            self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        # Verify that both namespace transitions return the expected application value.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        for key, (_value, _mods, result) in enumerate(self.modify_cases, 1):
            cursor.set_key(key)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), result)
        self.session.rollback_transaction()

    def test_stable_modify_into_namespace(self):
        self.check_modify_namespace_transitions(self.session.open_cursor(self.uri))

    def test_ingest_modify_into_namespace(self):
        self.session.checkpoint()
        self.reopen_disagg_conn('disaggregated=(role="follower"),')
        self.check_modify_namespace_transitions(self.session.open_cursor(self.uri))
