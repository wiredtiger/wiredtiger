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
from helper_disagg import disagg_test_class
from wtscenario import make_scenarios

# Search conflicts must leave subsequent same-cursor operations consistent with ordinary cursors.

@disagg_test_class
class test_layered_prepare02(wttest.WiredTigerTestCase):

    test_name = __qualname__
    scenarios = make_scenarios([
        ('search',      dict(use_search_near=False)),
        ('search_near', dict(use_search_near=True)),
    ], [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ])

    conn_base_config = 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    def step(self, cursor):
        return cursor.next() if self.forward else cursor.prev()

    def safe_step(self, cursor):
        try:
            return self.step(cursor)
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def safe_search(self, cursor, key):
        cursor.set_key(key)
        try:
            return cursor.search()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def safe_search_near(self, cursor, key):
        cursor.set_key(key)
        try:
            return cursor.search_near()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def conflict_search(self, cursor, key):
        if self.use_search_near:
            return self.safe_search_near(cursor, key)
        return self.safe_search(cursor, key)

    def _setup_follower(self, prepared_key, suffix='', with_reference=False, base_ts=0):
        uri = f'table:{self.test_name}{suffix}'
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')

        stable_keys = ['1', '2', '3']
        self.session.create(uri, 'key_format=S,value_format=S,block_manager=disagg,type=layered')
        with self.transaction(session=self.session, commit_timestamp=base_ts + 100):
            c = self.session.open_cursor(uri)
            for k in stable_keys:
                c[k] = 'stable_' + k
            c.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(base_ts + 200)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        prep_session = conn_follow.open_session('')
        reference_uri = f'table:reference{suffix}'
        if with_reference:
            prep_session.create(reference_uri, 'key_format=S,value_format=S')
            with self.transaction(session=prep_session, commit_timestamp=base_ts + 250):
                reference = prep_session.open_cursor(reference_uri)
                for k in stable_keys:
                    reference[k] = 'stable_' + k
                reference.close()
        prep_cursor = prep_session.open_cursor(uri)
        prep_session.begin_transaction()
        prep_cursor[prepared_key] = 'prepared_update'
        prep_cursor.close()
        if with_reference:
            reference = prep_session.open_cursor(reference_uri)
            reference[prepared_key] = 'prepared_update'
            reference.close()
        prep_session.prepare_transaction(
                f'prepare_timestamp={self.timestamp_str(base_ts + 300)}'
                + f',prepared_id={self.prepared_id_str(1)}')

        # Reader whose read_timestamp covers the prepare so it sees the conflict.
        iter_session = conn_follow.open_session('')
        iter_session.begin_transaction(f'read_timestamp={self.timestamp_str(base_ts + 400)}')
        iter_cursor = iter_session.open_cursor(uri)
        reference = iter_session.open_cursor(reference_uri) if with_reference else None

        return stable_keys, conn_follow, prep_session, iter_session, iter_cursor, reference

    def assert_scan(self, cursor, expected):
        actual = []
        for pair in expected:
            self.assertEqual(self.step(cursor), 0)
            actual.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(actual[-1], pair)
        self.assertEqual(self.step(cursor), wiredtiger.WT_NOTFOUND)
        self.assertEqual(actual, expected)

    def test_iterate_after_prepare_conflict(self):
        # A failed search must discard the previous position.
        stable_keys, conn_follow, prep_session, iter_session, cursor, _ = \
            self._setup_follower('2')

        self.assertEqual(self.safe_search(cursor, '3' if self.forward else '1'), 0)
        for _ in range(2):
            self.assertEqual(self.conflict_search(cursor, '2'), wiredtiger.WT_PREPARE_CONFLICT)

        # After rolling back the prepare, iteration must return all stable keys.
        prep_session.rollback_transaction()
        keys = stable_keys if self.forward else list(reversed(stable_keys))
        self.assert_scan(cursor, [(k, 'stable_' + k) for k in keys])

        cursor.close()
        iter_session.rollback_transaction()
        iter_session.close()
        prep_session.close()
        conn_follow.close()

    def test_iterate_after_conflicting_step_then_search(self):
        # A failed search after failed iteration must restart the walk.
        prepared_key = '1' if self.forward else '3'
        stable_keys, conn_follow, prep_session, iter_session, cursor, _ = \
            self._setup_follower(prepared_key)

        for _ in range(2):
            self.assertEqual(self.safe_step(cursor), wiredtiger.WT_PREPARE_CONFLICT)
            self.assertEqual(self.conflict_search(cursor, prepared_key),
                wiredtiger.WT_PREPARE_CONFLICT)

        # After rolling back the prepare, iteration must return all stable keys.
        prep_session.rollback_transaction()
        keys = stable_keys if self.forward else list(reversed(stable_keys))
        self.assert_scan(cursor, [(k, 'stable_' + k) for k in keys])

        cursor.close()
        iter_session.rollback_transaction()
        iter_session.close()
        prep_session.close()
        conn_follow.close()

    def test_same_cursor_operations_after_search_conflict(self):
        for index, operation in enumerate(('search', 'update', 'remove', 'modify', 'reserve', 'reset')):
            with self.subTest(operation=operation):
                keys, conn_follow, prepared, reader, cursor, reference = \
                    self._setup_follower('2', '_' + operation, with_reference=True,
                        base_ts=index * 500)
                key = '1' if self.forward else '3'
                for c in (cursor, reference):
                    for _ in range(2):
                        self.assertEqual(self.conflict_search(c, '2'),
                            wiredtiger.WT_PREPARE_CONFLICT)
                    if operation == 'reset':
                        self.assertEqual(c.reset(), 0)
                    else:
                        c.set_key(key)
                        if operation == 'search':
                            self.assertEqual(c.search(), 0)
                            self.assertEqual((c.get_key(), c.get_value()), (key, 'stable_' + key))
                        elif operation == 'update':
                            c.set_value('updated')
                            self.assertEqual(c.update(), 0)
                        elif operation == 'modify':
                            self.assertEqual(c.modify([wiredtiger.Modify('X', 0, 1)]), 0)
                        elif operation == 'remove':
                            self.assertEqual(c.remove(), 0)
                        else:
                            self.assertEqual(c.reserve(), 0)
                prepared.rollback_transaction()
                check = reader.open_cursor(f'table:{self.test_name}_{operation}')
                check.set_key(key)
                self.assertEqual(check.search(),
                    wiredtiger.WT_NOTFOUND if operation == 'remove' else 0)
                if operation != 'remove':
                    value = 'updated' if operation == 'update' else \
                        'Xtable_' + key if operation == 'modify' else 'stable_' + key
                    self.assertEqual((check.get_key(), check.get_value()), (key, value))
                check.close()
                expected = []
                while True:
                    ret = self.step(reference)
                    if ret != 0:
                        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
                        break
                    expected.append((reference.get_key(), reference.get_value()))
                    self.assertLessEqual(len(expected), 3)
                ordered = keys if self.forward else list(reversed(keys))
                if operation != 'reset':
                    ordered = ordered[1:]
                self.assertEqual(expected, [(k, 'stable_' + k) for k in ordered])
                self.assert_scan(cursor, expected)
                cursor.close()
                reference.close()
                reader.rollback_transaction()
                reader.close()
                prepared.close()
                conn_follow.close()
