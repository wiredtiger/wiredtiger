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
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# Layered iteration preserves fixed-timestamp visibility across multi-key prepare resolution.

@disagg_test_class
class test_layered_prepare01(LayeredStepdownMixin, wttest.WiredTigerTestCase):

    test_name = __qualname__
    prepare_in_window = False
    scenarios = make_scenarios([
        ('only_key',  dict(stable_keys=['1'],         prepared_keys=['1'])),
        ('first',     dict(stable_keys=['1','2','3'], prepared_keys=['1'])),
        ('middle',    dict(stable_keys=['1','2','3'], prepared_keys=['2'])),
        ('last',      dict(stable_keys=['1','2','3'], prepared_keys=['3'])),
        ('first_two', dict(stable_keys=['1','2','3'], prepared_keys=['1','2'])),
        ('last_two',  dict(stable_keys=['1','2','3'], prepared_keys=['2','3'])),
        ('outer_two', dict(stable_keys=['1','2','3'], prepared_keys=['1','3'])),
    ], [
        ('rollback', dict(commit_ts=None, read_ts=400)),
        ('below_prepare', dict(commit_ts=350, read_ts=299)),
        ('at_prepare', dict(commit_ts=300, read_ts=300)),
        ('visible_commit', dict(commit_ts=350, read_ts=400)),
        ('future_commit', dict(commit_ts=450, read_ts=400)),
    ], [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ], [
        ('insert', dict(operation='insert')),
        ('update', dict(operation='update')),
        ('remove', dict(operation='remove')),
    ], [
        ('follower_ingest', dict(on_leader=False, stepdown=False, mirroring=False)),
        ('leader_stable', dict(on_leader=True, stepdown=False, mirroring=False)),
        ('window_unmirrored', dict(on_leader=True, stepdown=True, mirroring=False)),
        ('window_mirrored', dict(on_leader=True, stepdown=True, mirroring=True)),
        ('window_ingest_unmirrored',
            dict(on_leader=True, stepdown=True, mirroring=False, prepare_in_window=True)),
        ('window_ingest_mirrored',
            dict(on_leader=True, stepdown=True, mirroring=True, prepare_in_window=True)),
    ], include=lambda _, s:
        (s.get('prepare_in_window', False) and s['read_ts'] == 400 and
            s['commit_ts'] in (None, 350)) or
        (not s.get('prepare_in_window', False) and
            (not s['stepdown'] or s['commit_ts'] is None)))

    conn_base_config = 'precise_checkpoint=true,debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader",stepdown_write_mirroring=' + \
            str(self.mirroring).lower() + ')'

    def test_iterate_after_prepare_resolution(self):
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        uri = f'table:{self.test_name}'
        self.session.create(uri, 'key_format=S,value_format=S,block_manager=disagg,type=layered')

        original = {k: 'original_' + k for k in self.stable_keys
            if self.operation != 'insert' or k not in self.prepared_keys}
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(uri)
            for k, value in original.items():
                c[k] = value
            c.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(200)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        step_down_ts = 250 if self.prepare_in_window else 325
        if self.prepare_in_window:
            self.set_step_down_ts(step_down_ts)
            self.write_at(uri, {'0': 'ingest_0', '4': 'ingest_4'}, 275)
            original.update({'0': 'ingest_0', '4': 'ingest_4'})

        connection = self.conn if self.on_leader else conn_follow
        prep_session = connection.open_session('')
        prep_cursor = prep_session.open_cursor(uri, None, 'overwrite=false')
        prep_session.begin_transaction()
        for k in self.prepared_keys:
            prep_cursor.set_key(k)
            if self.operation == 'remove':
                self.assertEqual(prep_cursor.remove(), 0)
            else:
                prep_cursor.set_value('prepared_' + k)
                op = prep_cursor.insert if self.operation == 'insert' else prep_cursor.update
                self.assertEqual(op(), 0)
        prep_cursor.close()
        prep_session.prepare_transaction(
                f'prepare_timestamp={self.timestamp_str(300)}'
                + f',prepared_id={self.prepared_id_str(1)}')

        if self.stepdown and not self.prepare_in_window:
            self.set_step_down_ts(step_down_ts)
            self.write_at(uri, {'0': 'ingest_0', '4': 'ingest_4'}, 350)
            original.update({'0': 'ingest_0', '4': 'ingest_4'})

        iter_session = connection.open_session('')
        iter_session.begin_transaction(f'read_timestamp={self.timestamp_str(self.read_ts)}')
        cursor = iter_session.open_cursor(uri)
        step = cursor.next if self.forward else cursor.prev

        visible = original.copy()
        if self.commit_ts is not None and self.commit_ts <= self.read_ts:
            for k in self.prepared_keys:
                if self.operation == 'remove':
                    del visible[k]
                else:
                    visible[k] = 'prepared_' + k
        expected = sorted(visible.items(), reverse=not self.forward)
        first_prepared = min(self.prepared_keys) if self.forward else max(self.prepared_keys)
        expected_prefix = sorted(((k, v) for k, v in original.items()
            if (k < first_prepared if self.forward else k > first_prepared)),
            reverse=not self.forward) if self.read_ts >= 300 else expected
        prefix = []
        for pair in expected_prefix:
            self.assertEqual(step(), 0)
            prefix.append((cursor.get_key(), cursor.get_value()))
            self.assertEqual(prefix[-1], pair)
        self.assertEqual(prefix, expected_prefix)

        if self.read_ts >= 300:
            for _ in range(2):
                self.assertRaisesException(wiredtiger.WiredTigerError, step,
                    '/conflict with a prepared update/')
            if prefix:
                reverse = cursor.prev if self.forward else cursor.next
                if self.prepare_in_window and self.mirroring:
                    for _ in range(2):
                        self.assertRaisesException(wiredtiger.WiredTigerError, reverse,
                            '/conflict with a prepared update/')
                else:
                    self.assertEqual(reverse(), 0)
                    self.assertEqual((cursor.get_key(), cursor.get_value()), prefix[-1])
                for _ in range(2):
                    self.assertRaisesException(wiredtiger.WiredTigerError, step,
                        '/conflict with a prepared update/')
        else:
            self.assertEqual(step(), wiredtiger.WT_NOTFOUND)

        if self.commit_ts is not None:
            prep_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(self.commit_ts)},'
                f'durable_timestamp={self.timestamp_str(self.commit_ts)}')
        else:
            prep_session.rollback_transaction()
        if self.on_leader and not self.stepdown and self.commit_ts is not None:
            self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.commit_ts)}')

        suffix = []
        if self.read_ts >= 300:
            for pair in expected[len(expected_prefix):]:
                self.assertEqual(step(), 0)
                suffix.append((cursor.get_key(), cursor.get_value()))
                self.assertEqual(suffix[-1], pair)
            self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(suffix, expected[len(expected_prefix):])
        self.assertEqual(prefix + suffix, expected)

        for k in self.stable_keys:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), 0 if k in visible else wiredtiger.WT_NOTFOUND)
            if k in visible:
                self.assertEqual((cursor.get_key(), cursor.get_value()), (k, visible[k]))
        cursor.close()
        iter_session.rollback_transaction()
        prep_session.close()
        iter_session.close()
        conn_follow.close()
        if self.stepdown:
            self.complete_step_down(step_down_ts)
