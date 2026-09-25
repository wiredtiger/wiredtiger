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

# test_layered_arm05.py
#    An armed reader on a leader with a step-down armed meets another transaction's prepared
#    updates. The reader sees what it would see on a plain table: a read timestamp at or above the
#    prepare timestamp returns WT_PREPARE_CONFLICT at the prepared key, again on retry, and resumes
#    at that key once the prepared transaction resolves; a read timestamp below it skips the
#    prepared update. A scan may report the conflict before keys that precede the prepared key,
#    but returns every key exactly once and in order across the conflict. The prepared transaction
#    is either armed, so its updates are in both constituents, or began before the arm, so they are
#    in stable alone.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

class LayeredArmPreparedBase(LayeredStepdownMixin):
    conn_config = 'statistics=(all),precise_checkpoint=true,disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    prepared_by = [
        ('armed', dict(prepared_armed=True)),
        ('straddler', dict(prepared_armed=False)),
    ]
    resolutions = [
        ('commit', dict(commit=True)),
        ('rollback', dict(commit=False)),
    ]
    # Mirrored keys either next to the prepared keys, so a scan steps both constituents onto a
    # prepared key at once, or separated from them by a key in stable alone.
    layouts = [
        ('adjacent', dict(armed_keys=('k02', 'k07'))),
        ('gap', dict(armed_keys=('k01', 'k08'))),
    ]

    uri = 'layered:test_layered_arm05'
    prepared_keys = ('k03', 'k06')

    def expected(self, resolved):
        kv = {f'k{i:02}': 'base' for i in range(10)}
        kv.update({k: 'armed' for k in self.armed_keys})
        if resolved and self.commit:
            kv.update({k: 'prep' for k in self.prepared_keys})
        return kv

    def step(self, cursor, forward):
        try:
            ret = cursor.next() if forward else cursor.prev()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return 'conflict'
            raise
        if ret == wiredtiger.WT_NOTFOUND:
            return None
        self.assertEqual(ret, 0)
        return (cursor.get_key(), cursor.get_value())

    def search(self, cursor, key):
        cursor.set_key(key)
        try:
            self.assertEqual(cursor.search(), 0)
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return 'conflict'
            raise
        return cursor.get_value()

    # Committed data in stable alone and mirrored armed writes around the keys the prepared
    # transaction will update, so a scan in either direction crosses a prepared key with both
    # constituents positioned.
    def setup_prepared(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {f'k{i:02}': 'base' for i in range(10)}, 10)
        self.checkpoint_at(10)

        prepared = self.conn.open_session()
        cursor = prepared.open_cursor(self.uri)
        if not self.prepared_armed:
            prepared.begin_transaction()
        self.arm()
        self.write_at(self.uri, {k: 'armed' for k in self.armed_keys}, 15)
        if self.prepared_armed:
            prepared.begin_transaction()
        for k in self.prepared_keys:
            cursor[k] = 'prep'
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.timestamp_str(1))

        ingest = self.read_keys_at(self.ingest_uri(self.uri), 15)
        self.assertEqual(ingest, set(self.armed_keys))
        return prepared, cursor

    def resolve(self, prepared, cursor):
        if self.commit:
            prepared.commit_transaction('commit_timestamp=' + self.timestamp_str(22) +
                ',durable_timestamp=' + self.timestamp_str(22))
        else:
            prepared.rollback_transaction()
        cursor.close()
        prepared.close()
        self.checkpoint_at(25)

    def scan_across_prepared(self, forward):
        prepared, pcursor = self.setup_prepared()

        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(25))

        blocked = self.prepared_keys[0] if forward else self.prepared_keys[-1]
        order = sorted(self.expected(False), reverse=not forward)
        seen = []
        while (r := self.step(cursor, forward)) != 'conflict':
            self.assertIsNotNone(r, 'scan ended without meeting the prepared update')
            seen.append(r)
        self.assertLessEqual(len(seen), order.index(blocked))
        self.assertEqual(seen, [(k, self.expected(False)[k]) for k in order[:len(seen)]])
        for _ in range(2):
            self.assertEqual(self.step(cursor, forward), 'conflict')

        self.resolve(prepared, pcursor)

        rest = []
        while (r := self.step(cursor, forward)) is not None:
            rest.append(r)
        self.assertEqual(rest, [(k, self.expected(True)[k]) for k in order[len(seen):]])
        reader.rollback_transaction()

        # A fresh scan in the opposite direction sees the resolved values.
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        rest = []
        while (r := self.step(cursor, not forward)) is not None:
            rest.append(r)
        self.assertEqual(rest, sorted(self.expected(True).items(), reverse=forward))
        reader.rollback_transaction()
        cursor.close()
        reader.close()


@disagg_test_class
class test_layered_arm05(LayeredArmPreparedBase, wttest.WiredTigerTestCase):
    scenarios = make_scenarios(LayeredArmPreparedBase.disagg_storages,
        LayeredArmPreparedBase.prepared_by, LayeredArmPreparedBase.resolutions,
        LayeredArmPreparedBase.layouts)

    def test_next_across_prepared(self):
        self.scan_across_prepared(True)

    def test_prev_across_prepared(self):
        self.scan_across_prepared(False)

    def test_search_prepared(self):
        prepared, pcursor = self.setup_prepared()

        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        for k in self.prepared_keys:
            self.assertEqual(self.search(cursor, k), 'conflict')
        self.assertEqual(self.search(cursor, 'k04'), 'base')
        self.assertEqual(self.search(cursor, self.armed_keys[-1]), 'armed')
        self.assertEqual(self.search(cursor, self.prepared_keys[0]), 'conflict')

        self.resolve(prepared, pcursor)
        for k in self.prepared_keys:
            self.assertEqual(self.search(cursor, k), self.expected(True)[k])
        reader.rollback_transaction()
        cursor.close()
        reader.close()

    def test_read_below_prepare(self):
        prepared, pcursor = self.setup_prepared()

        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        for forward in (True, False):
            reader.begin_transaction('read_timestamp=' + self.timestamp_str(15))
            rows = []
            while (r := self.step(cursor, forward)) is not None:
                rows.append(r)
            self.assertEqual(rows, sorted(self.expected(False).items(), reverse=not forward))
            reader.rollback_transaction()
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        for k in self.prepared_keys:
            self.assertEqual(self.search(cursor, k), 'base')
        reader.rollback_transaction()

        self.resolve(prepared, pcursor)
        cursor.close()
        reader.close()

# A prepared transaction that is not armed blocks the demotion, so only an armed one can leave a
# scan blocked across it.
@disagg_test_class
class test_layered_arm05_demote(LayeredArmPreparedBase, wttest.WiredTigerTestCase):
    prepared_armed = True
    scenarios = make_scenarios(LayeredArmPreparedBase.disagg_storages,
        LayeredArmPreparedBase.resolutions, LayeredArmPreparedBase.layouts)

    # The node demotes while an armed scan is blocked. The scan's snapshot spans the demotion, and
    # it resumes over the follower's view once the prepared transaction resolves there.
    def test_scan_blocked_across_demote(self):
        prepared, pcursor = self.setup_prepared()

        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        order = sorted(self.expected(False))
        seen = []
        while (r := self.step(cursor, True)) != 'conflict':
            self.assertIsNotNone(r)
            seen.append(r)

        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.step(cursor, True), 'conflict')

        if self.commit:
            prepared.commit_transaction('commit_timestamp=' + self.timestamp_str(22) +
                ',durable_timestamp=' + self.timestamp_str(22))
        else:
            prepared.rollback_transaction()
        pcursor.close()
        prepared.close()
        # Its updates in the stepped-down stable tree are left behind; ingest carries them.
        self.assertEqual(
            self.conn_stat(stat.conn.txn_prepared_updates_stepped_down), len(self.prepared_keys))

        rest = []
        while (r := self.step(cursor, True)) is not None:
            rest.append(r)
        self.assertEqual(seen + rest,
            [(k, self.expected(k not in dict(seen))[k]) for k in order])
        reader.rollback_transaction()
        cursor.close()
        reader.close()
        self.assertEqual(self.read_kvs_at(self.uri, 25), self.expected(True))

if __name__ == '__main__':
    wttest.run()
