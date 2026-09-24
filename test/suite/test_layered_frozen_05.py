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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_frozen_05.py
#    Readers spanning the demotion. A timestamped cursor already positioned on the live tree keeps
#    reading. New readers after the demotion, timestamped or not, see every commit. A snapshot taken before the
#    demotion that first reads the table after it is served when it carries a read timestamp, and
#    refused with WT_ROLLBACK when it does not, rather than reading the wrong content.
@disagg_test_class
class test_layered_frozen_05(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # Whether the readers that span the demotion carry a read timestamp.
    readers = [
        ('timestamped', dict(read_config='read_timestamp=1e')),
        ('untimestamped', dict(read_config=None)),
    ]
    scenarios = make_scenarios(disagg_storages, readers)

    uri = 'layered:test_layered_frozen_05'
    other_uri = 'layered:test_layered_frozen_05_other'

    base = {f'k{i}': 'base' for i in range(0, 10, 2)}
    window = {f'k{i}': 'window' for i in range(1, 10, 2)}

    def setup_window(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session.create(self.other_uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, self.base, 10)
        self.write_at(self.other_uri, {'x': 'x'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, self.window, 30)

    def test_positioned_reader_continues(self):
        self.setup_window()
        expected = {**self.base, **self.window}

        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        reader.begin_transaction(self.read_config)
        seen = []
        for _ in range(3):
            self.assertEqual(cursor.next(), 0)
            seen.append((cursor.get_key(), cursor.get_value()))

        live_refused, bind_refused = self.refusal_counts()
        self.demote()
        try:
            while cursor.next() == 0:
                seen.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(dict(seen), expected)
            self.assertEqual([k for k, _ in seen], sorted(expected))
            cursor.set_key('k5')
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'window')
            refused = False
        except wiredtiger.WiredTigerError as e:
            # The role change swaps what the stable content is, so an untimestamped snapshot may be
            # refused when the cursor next binds it. A timestamped reader must never be refused.
            if self.read_config is not None or not self.is_rollback(e):
                raise
            self.ignoreStderrPatternIfExists('WT_ROLLBACK')
            refused = True
        reader.rollback_transaction()
        cursor.close()
        reader.close()
        self.assertEqual(self.refusal_counts(),
            (live_refused, bind_refused + 1 if refused else bind_refused))

    def test_new_readers_after_demote(self):
        self.setup_window()
        self.demote()
        refusals = self.refusal_counts()

        expected = {**self.base, **self.window}
        self.assertEqual(self.read_kvs_at(self.uri, 30), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 25), self.base)
        self.assertEqual(self.read_kvs(self.uri), expected)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction(self.read_config)
        cursor.set_key('k3')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'window')
        self.session.rollback_transaction()
        cursor.close()
        self.assertEqual(self.refusal_counts(), refusals)

    def test_snapshot_from_before_demote_binds_after(self):
        self.setup_window()

        # Establish the snapshot before the demotion through another table, so the first read of
        # this table happens after it.
        reader = self.conn.open_session()
        reader.begin_transaction(self.read_config)
        other = reader.open_cursor(self.other_uri)
        self.assertEqual(other['x'], 'x')

        live_refused, bind_refused = self.refusal_counts()
        self.demote()

        cursor = reader.open_cursor(self.uri)
        if self.read_config is None:
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.next(),
                wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
            self.assertGreater(self.connection_stat(wiredtiger.stat.conn.layered_curs_open_stable_refused),
                bind_refused)
        else:
            found = {}
            while cursor.next() == 0:
                found[cursor.get_key()] = cursor.get_value()
            self.assertEqual(found, {**self.base, **self.window})
            self.assertEqual(self.refusal_counts(), (live_refused, bind_refused))
        self.assertEqual(self.connection_stat(wiredtiger.stat.conn.layered_stable_live_open_refused),
            live_refused)
        reader.rollback_transaction()
        cursor.close()
        other.close()
        reader.close()
