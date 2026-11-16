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


@disagg_test_class
class test_layered_async_stepdown26(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('next', dict(forward=True)), ('prev', dict(forward=False))], [
        ('unmirrored', dict(mirroring=False)), ('mirrored', dict(mirroring=True))], [
        ('insert', dict(shared_key=False)), ('shared_key', dict(shared_key=True))], [
        ('resolved_early', dict(resolve_early=True)), ('unresolved', dict(resolve_early=False))])

    def conn_config(self):
        return 'statistics=(all),precise_checkpoint=true,' \
            'debug_mode=(disagg_stepdown_prepare=true),' \
            'disaggregated=(role="leader",stepdown_write_mirroring=' + \
            str(self.mirroring).lower() + ')'

    def test_prepare_resolved_before_merge_winner(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        values = {f'key{rank:02}': f'value{rank:02}' for rank in range(0, 20, 2)}
        if self.shared_key:
            values['key09'] = 'stable'
        self.write_at(self.uri, values, 10)
        self.set_global_ts(1, 10)
        self.set_step_down_ts(15)

        prepared = self.conn.open_session()
        prepare_cursor = prepared.open_cursor(self.uri)
        prepared.begin_transaction()
        prepare_cursor['key09'] = 'prepared'
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        cursor = self.session.open_cursor(self.uri)
        step = cursor.next if self.forward else cursor.prev
        expected = sorted(values.items(), reverse=not self.forward)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        self.assertEqual(step(), 0)
        actual = [(cursor.get_key(), cursor.get_value())]
        self.assertEqual(actual, expected[:1])
        if self.resolve_early:
            prepared.rollback_transaction()

        for _ in range(4):
            self.assertEqual(step(), 0)
            actual.append((cursor.get_key(), cursor.get_value()))
        self.assertEqual(actual, expected[:5])

        if not self.resolve_early:
            for _ in range(2):
                self.assertRaisesException(wiredtiger.WiredTigerError, step,
                    '/conflict with a prepared update/')
            prepared.rollback_transaction()

        for item in expected[5:]:
            self.assertEqual(step(), 0)
            actual.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(actual[-1], item)
        self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(actual, expected)
        self.session.rollback_transaction()
        cursor.close()
        prepare_cursor.close()
        prepared.close()
        if self.mirroring:
            self.complete_step_down(15)


@disagg_test_class
class test_layered_async_stepdown26_reversal(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('next', dict(forward=True)), ('prev', dict(forward=False))], [
        ('insert', dict(shared_key=False)), ('shared_key', dict(shared_key=True))])
    conn_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),' \
        'disaggregated=(role="leader",stepdown_write_mirroring=false)'

    def key(self, rank):
        return f'key{rank if self.forward else 100 - rank:02}'

    def test_continue_after_alternate_positioning_conflict(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        values = {self.key(rank): 'stable' for rank in (70, 90)}
        self.write_at(self.uri, values, 10)
        self.set_global_ts(1, 10)

        stable_session = self.conn.open_session()
        stable_cursor = stable_session.open_cursor(self.uri)
        stable_session.begin_transaction()
        stable_cursor[self.key(70)] = 'prepared-stable'
        stable_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))
        self.set_step_down_ts(15)
        if self.shared_key:
            self.write_at(self.uri, {self.key(90): 'ingest90'}, 18)

        ingest_session = self.conn.open_session()
        ingest_cursor = ingest_session.open_cursor(self.uri)
        ingest_session.begin_transaction()
        ingest_cursor[self.key(40)] = 'ingest'
        ingest_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        cursor = self.session.open_cursor(self.uri)
        step = cursor.next if self.forward else cursor.prev
        reverse = cursor.prev if self.forward else cursor.next
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        self.assertRaisesException(wiredtiger.WiredTigerError, step,
            '/conflict with a prepared update/')
        self.assertRaisesException(wiredtiger.WiredTigerError, reverse,
            '/conflict with a prepared update/')
        ingest_session.commit_transaction('commit_timestamp=' + self.timestamp_str(25) +
            ',durable_timestamp=' + self.timestamp_str(25))
        stable_session.rollback_transaction()

        actual = []
        for rank, value in ((40, 'ingest'), (70, 'stable'),
            (90, 'ingest90' if self.shared_key else 'stable')):
            self.assertEqual(step(), 0)
            actual.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(actual[-1], (self.key(rank), value))
        self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        cursor.close()
        stable_cursor.close()
        stable_session.close()
        ingest_cursor.close()
        ingest_session.close()


@disagg_test_class
class test_layered_async_stepdown26_search(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), [
        ('next', dict(forward=True)), ('prev', dict(forward=False))], [
        ('search', dict(near=False)), ('search_near', dict(near=True))], [
        ('unmirrored', dict(mirroring=False)), ('mirrored', dict(mirroring=True))])

    def conn_config(self):
        return 'statistics=(all),precise_checkpoint=true,' \
            'debug_mode=(disagg_stepdown_prepare=true),' \
            'disaggregated=(role="leader",stepdown_write_mirroring=' + \
            str(self.mirroring).lower() + ')'

    def key(self, rank):
        return f'key{rank if self.forward else 60 - rank:02}'

    def test_search_replaces_blocked_walk_after_reopen(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {self.key(rank): f'value{rank}'
            for rank in (10, 20, 30, 40, 50)}, 10)
        self.set_global_ts(1, 10)

        prepared = self.conn.open_session()
        prepare_cursor = prepared.open_cursor(self.uri)
        prepared.begin_transaction()
        prepare_cursor[self.key(20)] = 'prepared'
        prepared.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))
        self.set_step_down_ts(15)

        cursor = self.session.open_cursor(self.uri)
        step = cursor.next if self.forward else cursor.prev
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        self.assertEqual(step(), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(10), 'value10'))
        self.assertRaisesException(wiredtiger.WiredTigerError, step,
            '/conflict with a prepared update/')

        try:
            self.complete_step_down(15)
            cursor.set_key(self.key(40))
            self.assertEqual(cursor.search_near() if self.near else cursor.search(), 0)
            self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(40), 'value40'))
            self.assertEqual(step(), 0)
            self.assertEqual((cursor.get_key(), cursor.get_value()), (self.key(50), 'value50'))
            self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        finally:
            self.session.rollback_transaction()
            cursor.close()
            try:
                prepared.rollback_transaction()
            except wiredtiger.WiredTigerError:
                self.assertEqual(prepared.get_last_error()[1], wiredtiger.WT_CONFLICT_DISAGG)
            prepare_cursor.close()
            prepared.close()
