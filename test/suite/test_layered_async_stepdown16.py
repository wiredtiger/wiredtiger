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
from wtscenario import make_scenarios

# A walk over a layered table tracks a position in each constituent. A prepared update blocks the
# constituent holding it: the walk keeps that position while the key it would return is withheld.
# Both constituents can carry prepared content, so both can block a walk, and either way the walk
# must report the conflict and then resume from where it stopped once the conflict is resolved.
#
# Prepared transactions are ordinarily rejected while the step-down timestamp is set, so this test
# uses the debug_mode.disagg_stepdown_prepare knob to bypass that ban and exercise the walk logic
# directly; that knob is temporary and goes away once prepared transactions are supported across a
# step-down for real. It resolves the blocking transaction by rollback rather than commit: the
# knob does not affect the step-down straddler check, so a straddling prepared transaction can
# still only commit into a panic.
@disagg_test_class
class test_layered_async_stepdown16(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    # Where the blocking prepared update lives. Content written before the step-down timestamp is
    # announced goes to the stable constituent; content written after it goes to ingest.
    prepare_targets = [
        ('prepare_on_stable', dict(prepare_before_step_down=True)),
        ('prepare_on_ingest', dict(prepare_before_step_down=False)),
    ]

    directions = [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ]

    # Whether a separate, ordinary key exists in both constituents at once, so the walk must step
    # past it without returning it twice. Every other key in this test belongs to exactly one
    # constituent, so without this a duplicate key is never part of the walk.
    tie_scenarios = [
        ('no_tie', dict(with_tie=False)),
        ('with_tie', dict(with_tie=True)),
    ]

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, prepare_targets, directions, tie_scenarios)

    test_name = __qualname__
    uri = f'layered:{test_name}'

    nrows = 40

    # The key the prepared update covers, placed in the middle so the walk reaches it with keys
    # both before and after it still to return.
    blocked_index = nrows // 2

    def keys(self):
        return [f'key{i:04d}' for i in range(self.nrows)]

    # Interleaving the two constituents' keys keeps both populated across the whole walk.
    def stable_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 0]

    def ingest_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 1]

    def set_global_ts(self, oldest, stable):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest) +
                                 ',stable_timestamp=' + self.timestamp_str(stable))

    def write_at(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    # Walk the whole table, tolerating the prepare conflict on the blocked key. Resolve the
    # blocking transaction by rollback on the first conflict, then keep walking: this is what
    # exercises resuming from a blocked position. Committing a straddling prepared transaction
    # panics the process by design, so this test resolves via rollback instead.
    def walk_resolving_conflict(self, cursor, resolve):
        step = cursor.next if self.forward else cursor.prev
        seen = []
        conflicts = 0
        resolved = False

        while True:
            try:
                if step() != 0:
                    break
            except wiredtiger.WiredTigerError as e:
                if wiredtiger.wiredtiger_strerror(
                        wiredtiger.WT_PREPARE_CONFLICT) not in str(e):
                    raise
                conflicts += 1
                # Bound the retries so a walk that never makes progress fails as a hang here
                # rather than spinning.
                self.assertLess(conflicts, 10,
                    'the walk never resumed past the prepared key')
                if not resolved:
                    resolve()
                    resolved = True
                continue
            seen.append(cursor.get_key())

        return seen, conflicts

    def test_walk_over_prepared_key(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = self.keys()
        blocked_key = keys[self.blocked_index]

        # A key distinct from every other key in the walk, given to both constituents so the walk
        # ties on it: the ingest copy shadows the stable one, like an ordinary layered-table update.
        tie_key = 'keytie'

        # Populate half the keys and make them stable, so a walk reads them from the stable
        # constituent.
        self.write_at(self.uri, {k: 'v-initial' for k in self.stable_keys()}, 10)
        if self.with_tie:
            self.write_at(self.uri, {tie_key: 'v-stable-original'}, 10)
        self.set_global_ts(1, 10)

        # A prepare before the announcement has to sit below it, like every write that precedes a
        # step-down; one after it is inside the window and sits above.
        prepare_ts = 12 if self.prepare_before_step_down else 30
        step_down_ts = 15

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)

        def prepare():
            prepare_session.begin_transaction()
            prepare_cursor[blocked_key] = 'v-prepared'
            prepare_session.prepare_transaction(
                'prepare_timestamp=' + self.timestamp_str(prepare_ts))

        if self.prepare_before_step_down:
            prepare()

        # Announce the step-down, then write the remaining keys so they reach the ingest
        # constituent. The walk needs a position in both constituents to consult either.
        self.conn.set_timestamp('step_down_timestamp=' + self.timestamp_str(step_down_ts))
        self.write_at(self.uri, {k: 'v-initial' for k in self.ingest_keys()}, 20)
        if self.with_tie:
            self.write_at(self.uri, {tie_key: 'v-ingest'}, 20)

        if not self.prepare_before_step_down:
            prepare()

        def resolve():
            prepare_session.rollback_transaction()

        # Read above the prepared timestamp so the prepared update is a conflict rather than
        # invisible content the walk may skip, and above the ingest keys so they are all visible.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))

        try:
            seen, conflicts = self.walk_resolving_conflict(cursor, resolve)
        finally:
            self.session.rollback_transaction()
            cursor.close()
            prepare_cursor.close()
            prepare_session.close()

        # The prepared key must have blocked the walk; otherwise the scenario never arose and the
        # rest of the assertions prove nothing.
        self.assertGreater(conflicts, 0,
            'the prepared update never blocked the walk')

        # The prepared transaction is rolled back rather than committed, uncovering the original
        # value underneath: every key is returned exactly once, in order, including the blocked one.
        # The tie key sorts after every other key, so it trails the walk in either direction.
        expected = keys + [tie_key] if self.with_tie else keys
        if not self.forward:
            expected = list(reversed(expected))
        self.assertEqual(seen, expected)

if __name__ == '__main__':
    wttest.run()
