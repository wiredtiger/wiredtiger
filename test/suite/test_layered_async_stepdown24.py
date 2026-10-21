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

# A walk blocked on a prepared update must resume exactly where it stopped, even when the blocked
# key resolves onto a key the other constituent shares, or when the walk's own transaction changes
# the table while the walk is blocked. The prepared updates are resolved by rollback: committing a
# transaction that straddles the step-down panics by design.
class BlockedWalkMixin(LayeredStepdownMixin):
    conn_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),disaggregated=(role="leader")'

    directions = [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ]

    uri = 'layered:test_layered_async_stepdown24'

    # Keys are named by rank in walk order, so one layout serves both directions.
    def key(self, rank):
        return f'key{rank if self.forward else 10 - rank}'

    def step(self, cursor):
        return cursor.next() if self.forward else cursor.prev()

    def prepare(self, rank, prepare_ts):
        session = self.conn.open_session()
        cursor = session.open_cursor(self.uri, None, None)
        session.begin_transaction()
        cursor[self.key(rank)] = 'prepared'
        session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(prepare_ts))
        return session

    # Close the session too: verification at teardown needs exclusive access to the table.
    def resolve(self, session):
        session.rollback_transaction()
        session.close()

    def assert_conflict(self, cursor):
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.step(cursor),
                                   '/conflict with a prepared update/')

    def assert_returns(self, cursor, rank):
        self.assertEqual(self.step(cursor), 0)
        self.assertEqual(cursor.get_key(), self.key(rank))

    def rest_of_walk(self, cursor):
        actual = []
        while self.step(cursor) == 0:
            actual.append((cursor.get_key(), cursor.get_value()))
        return actual

@disagg_test_class
class test_layered_async_stepdown24(BlockedWalkMixin, wttest.WiredTigerTestCase):
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), BlockedWalkMixin.directions)

    # Ingest blocks on rank 3, which stable also holds. Once ingest resolves onto rank 3, the walk
    # has to get stable past it too, and stable blocks on rank 4 while doing so. Each prepared
    # transaction is resolved as the walk reports its conflict, and rank 3 must still be returned.
    def test_tie_then_block_on_other_constituent(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {self.key(r): 'v' for r in (1, 3, 4, 5)}, 10)
        self.set_global_ts(1, 10)
        stable_prepare = self.prepare(4, 12)
        self.set_step_down_ts(15)
        self.write_at(self.uri, {self.key(2): 'v', self.key(3): 'v-ingest'}, 18)
        ingest_prepare = self.prepare(3, 22)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        unresolved = [ingest_prepare, stable_prepare]
        actual = []
        while True:
            try:
                if self.step(cursor) != 0:
                    break
            except wiredtiger.WiredTigerError as e:
                self.assertIn(
                    wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT), str(e))
                self.assertTrue(unresolved, 'the walk conflicted with no prepared update left')
                self.resolve(unresolved.pop(0))
                continue
            actual.append((cursor.get_key(), cursor.get_value()))
        self.session.rollback_transaction()

        self.assertEqual(unresolved, [], 'the walk did not block on both prepared updates')
        self.assertEqual(actual, [(self.key(1), 'v'), (self.key(2), 'v'),
            (self.key(3), 'v-ingest'), (self.key(4), 'v'), (self.key(5), 'v')])

@disagg_test_class
class test_layered_async_stepdown24_stall_writes(BlockedWalkMixin, wttest.WiredTigerTestCase):
    # Writes the walk's own transaction makes through another cursor while the walk is blocked on
    # stable, by rank: 2 sits between the last returned key and the blocked one, 4 between the
    # blocked key and the next ingest key, and 5 is the next ingest key itself. In the parked
    # scenarios ingest also holds rank 2, so the walk is blocked before returning it. As with a
    # plain table, a key written behind the blocked position is not returned.
    stall_writes = [
        ('insert_before_blocked', dict(ingest=[5], op='insert', rank=2,
            expected=[(3, 'v'), (5, 'v')])),
        ('insert_after_blocked', dict(ingest=[5], op='insert', rank=4,
            expected=[(3, 'v'), (4, 'new'), (5, 'v')])),
        ('remove_next_ingest', dict(ingest=[5], op='remove', rank=5, expected=[(3, 'v')])),
        ('update_next_ingest', dict(ingest=[5], op='insert', rank=5,
            expected=[(3, 'v'), (5, 'new')])),
        ('remove_parked_before_blocked', dict(ingest=[2, 5], op='remove', rank=2,
            expected=[(3, 'v'), (5, 'v')])),
        ('update_parked_before_blocked', dict(ingest=[2, 5], op='insert', rank=2,
            expected=[(2, 'new'), (3, 'v'), (5, 'v')])),
    ]

    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True),
        BlockedWalkMixin.directions, stall_writes)

    # Stable blocks on rank 3 right after the walk returns rank 1, before the walk returns any
    # ingest key. The walk's own write must be visible once the block resolves, including when the
    # walk is retried, and blocks again, between the write and the resolution.
    def test_write_while_stable_blocked(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {self.key(1): 'v', self.key(3): 'v'}, 10)
        self.set_global_ts(1, 10)
        stable_prepare = self.prepare(3, 12)
        self.set_step_down_ts(15)
        self.write_at(self.uri, {self.key(r): 'v' for r in self.ingest}, 18)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        self.assert_returns(cursor, 1)
        self.assert_conflict(cursor)

        writer = self.session.open_cursor(self.uri, None, None)
        writer.set_key(self.key(self.rank))
        if self.op == 'insert':
            writer.set_value('new')
            self.assertEqual(writer.insert(), 0)
        else:
            self.assertEqual(writer.remove(), 0)
        writer.close()

        self.assert_conflict(cursor)
        self.resolve(stable_prepare)

        actual = self.rest_of_walk(cursor)
        self.session.rollback_transaction()
        self.assertEqual(actual, [(self.key(r), v) for r, v in self.expected])

if __name__ == '__main__':
    wttest.run()
