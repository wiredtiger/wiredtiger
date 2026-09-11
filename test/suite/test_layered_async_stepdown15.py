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
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# Read routing and cursor positions during a planned step-down.
@disagg_test_class
class test_layered_async_stepdown15(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    write_modes = [
        ('mirrored', dict(write_mirroring=True)),
        ('ingest_only', dict(write_mirroring=False)),
    ]
    create_times = [
        ('existing', dict(window_created=False)),
        ('window_created', dict(window_created=True)),
    ]
    scenarios = make_scenarios(
        gen_disagg_storages(disagg_only=True), write_modes, create_times)
    uri = 'layered:stepdown_read_routing'

    def conn_config(self):
        return 'statistics=(all),disaggregated=(role=leader,stepdown_write_mirroring=' + \
            str(self.write_mirroring).lower() + ')'

    def populate(self):
        self.set_global_ts(1, 1)
        if self.window_created:
            self.set_step_down_ts(20)
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.initial = {k: b'original' for k in ('a', 'c', 'e', 'g', 'i')}
        self.write_at(self.uri, self.initial, 25 if self.window_created else 10)
        if not self.window_created:
            self.set_step_down_ts(20)
        self.assertEqual(self.stable_constituent_exists(self.conn, self.uri),
            not self.window_created)

    def ingest_stat(self, field):
        return self.constituent_stat(self.ingest_uri(self.uri), field)

    def constituent_stat(self, uri, field):
        c = self.session.open_cursor('statistics:' + uri)
        value = c[field][2]
        c.close()
        return value

    def read_stat(self, operation, constituent):
        if operation == 'next_random':
            operation = 'search_near'
        c = self.session.open_cursor('statistics:')
        value = c[getattr(stat.conn, 'layered_curs_' + operation + '_' + constituent)][2]
        c.close()
        return value

    def test_reads_open_only_needed_constituents(self):
        self.populate()
        self.write_at(self.uri, {'c': b'updated', 'f': b'inserted'}, 30)
        self.remove_at(self.uri, ['g'], 31)
        latest = dict(self.initial, c=b'updated', f=b'inserted')
        del latest['g']
        needs_ingest = not self.write_mirroring or self.window_created

        for read_ts, expected in ((27, self.initial), (40, latest)):
            for operation in ('search', 'search_near', 'next', 'prev', 'next_random'):
                config = 'next_random=true,next_random_seed=42' if operation == 'next_random' else None
                before_open = self.ingest_stat(stat.dsrc.cursor_open_count)
                before_read = self.read_stat(operation, 'ingest')
                before_stable_read = self.read_stat(operation, 'stable')
                cursor = self.session.open_cursor(self.uri, None, config)
                self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
                if operation in ('search', 'search_near'):
                    for key, value in expected.items():
                        cursor.set_key(key)
                        self.assertEqual(getattr(cursor, operation)(), 0)
                        self.assertEqual((cursor.get_key(), cursor.get_value()), (key, value))
                    cursor.set_key('g')
                    self.assertEqual(cursor.search(), 0 if read_ts == 27 else wiredtiger.WT_NOTFOUND)
                    cursor.set_key('d')
                    cmp = cursor.search_near()
                    self.assertIn(cursor.get_key(), ('c', 'e'))
                    self.assertEqual(cmp, -1 if cursor.get_key() == 'c' else 1)
                elif operation == 'next_random':
                    for _ in range(20):
                        self.assertEqual(cursor.next(), 0)
                        self.assertEqual(cursor.get_value(), expected[cursor.get_key()])
                else:
                    keys = sorted(expected, reverse=operation == 'prev')
                    for key in keys:
                        self.assertEqual(getattr(cursor, operation)(), 0)
                        self.assertEqual((cursor.get_key(), cursor.get_value()), (key, expected[key]))
                self.assertEqual(self.ingest_stat(stat.dsrc.cursor_open_count),
                    before_open + int(needs_ingest))
                if not needs_ingest:
                    self.assertEqual(self.read_stat(operation, 'ingest'), before_read)
                    if operation != 'next_random':
                        self.assertGreater(self.read_stat(operation, 'stable'), before_stable_read)
                self.session.rollback_transaction()
                cursor.close()

    def test_iteration_after_write(self):
        self.populate()
        for operation in ('update', 'modify', 'remove', 'reserve'):
            for forward in (True, False):
                for positioned in (True, False):
                    cursor = self.session.open_cursor(self.uri)
                    self.session.begin_transaction()
                    if positioned:
                        self.assertEqual(cursor.next(), 0)
                        self.assertEqual(cursor.next(), 0)
                        self.assertEqual(cursor.next(), 0)
                        self.assertEqual(cursor.get_key(), 'e')
                    else:
                        cursor.set_key('e')
                    if operation == 'update':
                        cursor.set_value(b'changed')
                        self.assertEqual(cursor.update(), 0)
                    elif operation == 'modify':
                        self.assertEqual(cursor.modify([wiredtiger.Modify(b'X', 0, 1)]), 0)
                    else:
                        self.assertEqual(getattr(cursor, operation)(), 0)

                    # An unpositioned remove stays unpositioned; other writes retain the key.
                    keys = ('g', 'i') if forward else ('c', 'a')
                    if operation == 'remove' and not positioned:
                        keys = ('a', 'c', 'g', 'i') if forward else ('i', 'g', 'c', 'a')
                    step = cursor.next if forward else cursor.prev
                    for key in keys:
                        self.assertEqual(step(), 0)
                        self.assertEqual(cursor.get_key(), key)
                    self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
                    self.session.rollback_transaction()
                    cursor.close()

    def test_bounded_read_write_walk(self):
        self.populate()
        for forward in (True, False):
            cursor = self.session.open_cursor(self.uri)
            cursor.set_key('c')
            cursor.bound('bound=lower')
            cursor.set_key('g')
            cursor.bound('bound=upper')
            self.session.begin_transaction()
            step = cursor.next if forward else cursor.prev
            for key in (('c', 'e', 'g') if forward else ('g', 'e', 'c')):
                self.assertEqual(step(), 0)
                self.assertEqual(cursor.get_key(), key)
                cursor.set_value(b'changed')
                self.assertEqual(cursor.update(), 0)
                self.assertEqual(cursor.get_value(), b'changed')
            self.assertEqual(step(), wiredtiger.WT_NOTFOUND)
            self.session.rollback_transaction()
            cursor.close()

    def test_write_position_reused(self):
        self.populate()
        for operation in ('update', 'modify', 'reserve'):
            for forward in (True, False):
                cursor = self.session.open_cursor(self.uri)
                self.session.begin_transaction()
                cursor.set_key('e')
                if operation == 'update':
                    cursor.set_value(b'changed')
                    self.assertEqual(cursor.update(), 0)
                    expected = b'changed'
                elif operation == 'modify':
                    self.assertEqual(cursor.modify([wiredtiger.Modify(b'X', 0, 1)]), 0)
                    expected = b'Xriginal'
                else:
                    self.assertEqual(cursor.reserve(), 0)
                    expected = b'original'
                self.assertEqual((cursor.get_key(), cursor.get_value()), ('e', expected))
                mirrored = self.write_mirroring and not self.window_created
                if mirrored:
                    before = self.constituent_stat(
                        self.stable_uri(self.uri), stat.dsrc.cursor_search_near)
                self.assertEqual(cursor.next() if forward else cursor.prev(), 0)
                self.assertEqual(cursor.get_key(), 'g' if forward else 'c')
                if mirrored:
                    self.assertEqual(self.constituent_stat(
                        self.stable_uri(self.uri), stat.dsrc.cursor_search_near), before)
                self.session.rollback_transaction()
                cursor.close()

    def test_chained_writes_and_remove(self):
        self.populate()
        self.ignoreStdoutPattern('stable table value in the tombstone namespace')
        for value in (b'plain', b'\x14\x14ab'):
            for forward in (True, False):
                cursor = self.session.open_cursor(self.uri)
                self.session.begin_transaction()
                cursor.set_key('e')
                cursor.set_value(value)
                self.assertEqual(cursor.update(), 0)
                self.assertEqual(cursor.get_value(), value)
                self.assertEqual(cursor.modify([wiredtiger.Modify(b'X', len(value), 0)]), 0)
                self.assertEqual(cursor.get_value(), value + b'X')
                self.assertEqual(cursor.remove(), 0)
                self.assertEqual(cursor.get_key(), 'e')
                self.assertEqual(cursor.next() if forward else cursor.prev(), 0)
                self.assertEqual(cursor.get_key(), 'g' if forward else 'c')
                cursor.set_key('e')
                self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
                self.session.rollback_transaction()
                cursor.close()

    def test_snapshot_excludes_later_commit(self):
        self.populate()
        reader = self.conn.open_session()
        cursor = reader.open_cursor(self.uri)
        reader.begin_transaction()
        self.assertEqual(cursor['c'], b'original')
        self.write_at(self.uri, {'c': b'updated', 'f': b'inserted'}, 30)
        self.remove_at(self.uri, ['g'], 31)
        self.assertEqual(cursor['c'], b'original')
        cursor.set_key('f')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.reset()
        self.assertEqual(dict(cursor), self.initial)
        reader.rollback_transaction()
        cursor.close()
        reader.close()

    def test_positioned_reader_across_step_down(self):
        self.populate()
        self.write_at(self.uri, {'c': b'updated', 'f': b'inserted'}, 30)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        self.assertEqual(cursor['c'], b'updated')
        self.complete_step_down(20)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'e')
        self.assertEqual(cursor.next(), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), ('f', b'inserted'))
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'e')
        self.assertEqual(cursor['c'], b'updated')
        self.session.rollback_transaction()
        cursor.close()

        self.write_at(self.uri, {'c': b'follower', 'h': b'new'}, 50)
        expected = dict(self.initial, c=b'follower', f=b'inserted', h=b'new')
        self.assertEqual(self.read_kvs_at(self.uri, 60), expected)

    def test_reuse_after_largest_key(self):
        self.populate()
        self.write_at(self.uri, {'z': b'last'}, 30)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(27))
        self.assertEqual(cursor.largest_key(), 0)
        self.assertEqual(cursor.get_key(), 'z')
        cursor.reset()
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'a')
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), 'i')
        self.session.rollback_transaction()
        cursor.close()
