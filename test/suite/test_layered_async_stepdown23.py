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

# Independent prepares must block retries until both constituents have resolved.
@disagg_test_class
class test_layered_async_stepdown23(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader",stepdown_write_mirroring=false)'

    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ], [
        ('stable_first', dict(stable_first=True)),
        ('ingest_first', dict(stable_first=False)),
    ], [
        ('rollback_stable_first', dict(rollback_stable_first=True)),
        ('rollback_ingest_first', dict(rollback_stable_first=False)),
    ], [
        ('fresh', dict(search_entry=False)),
        ('search', dict(search_entry=True)),
    ])

    test_name = __qualname__
    uri = f'layered:{test_name}'

    def test_two_independent_blocks_on_opposite_constituents(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.write_at(self.uri, {'key1': 'stable1', 'key4': 'stable4', 'key7': 'stable7'}, 10)
        self.set_global_ts(1, 10)

        stable_key_first = self.stable_first == self.forward
        stable_key = 'key3' if stable_key_first else 'key5'
        ingest_key = 'key5' if stable_key_first else 'key3'
        stable_prep_session = self.conn.open_session()
        stable_prep_cursor = stable_prep_session.open_cursor(self.uri, None, None)
        stable_prep_session.begin_transaction()
        stable_prep_cursor[stable_key] = 'prepared-stable'
        stable_prep_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        self.set_step_down_ts(15)
        self.write_at(self.uri, {'key2': 'ingest2', 'key6': 'ingest6'}, 18)

        ingest_prep_session = self.conn.open_session()
        ingest_prep_cursor = ingest_prep_session.open_cursor(self.uri, None, None)
        ingest_prep_session.begin_transaction()
        ingest_prep_cursor[ingest_key] = 'prepared-ingest'
        ingest_prep_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(22))

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        expected = [('key1', 'stable1'), ('key2', 'ingest2'), ('key4', 'stable4'),
            ('key6', 'ingest6'), ('key7', 'stable7')]
        if not self.forward:
            expected.reverse()
        step = cursor.next if self.forward else cursor.prev
        seen = []
        if self.search_entry:
            cursor.set_key(expected[0][0])
            self.assertEqual(cursor.search(), 0)
            seen.append((cursor.get_key(), cursor.get_value()))

        def walk_until_conflict():
            for _ in range(len(expected) + 1):
                try:
                    self.assertEqual(step(), 0, 'scan ended before the unresolved prepare')
                except wiredtiger.WiredTigerError as e:
                    self.assertIn('WT_PREPARE_CONFLICT', str(e))
                    return
                seen.append((cursor.get_key(), cursor.get_value()))
                self.assertEqual(seen, expected[:len(seen)])
            self.fail('scan did not reach the unresolved prepare')

        walk_until_conflict()
        self.assertEqual(seen, expected[:2])
        for _ in range(2):
            self.assertRaisesException(wiredtiger.WiredTigerError, step,
                '/conflict with a prepared update/')
        first, second = (stable_prep_session, ingest_prep_session) \
            if self.rollback_stable_first else (ingest_prep_session, stable_prep_session)
        first.rollback_transaction()
        walk_until_conflict()
        prefix_len = 3 if self.rollback_stable_first == self.stable_first else 2
        self.assertEqual(seen, expected[:prefix_len])
        for _ in range(2):
            self.assertRaisesException(wiredtiger.WiredTigerError, step,
                '/conflict with a prepared update/')
        second.rollback_transaction()
        for pair in expected[len(seen):]:
            self.assertEqual(step(), 0)
            seen.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(seen[-1], pair)
        self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(seen, expected)
        self.session.rollback_transaction()
        cursor.close()
        stable_prep_cursor.close()
        stable_prep_session.close()
        ingest_prep_cursor.close()
        ingest_prep_session.close()

if __name__ == '__main__':
    wttest.run()
