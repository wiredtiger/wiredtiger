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

# A walk over a layered table tracks a position in each constituent. A prepared update blocks the
# constituent holding it: the walk keeps that position while the key it would return is withheld.
# Both constituents can carry prepared content, so both can block a walk, and either way the walk
# must report the conflict and then resume from where it stopped once the conflict is resolved.
@disagg_test_class
class test_layered_async_stepdown16(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'

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

    def conn_config(self):
        return self.conn_base_config + \
            'disaggregated=(stepdown_write_mirroring=true,role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, prepare_targets, directions)

    test_name = __qualname__
    uri = f'layered:{test_name}'

    nrows = 40

    # The key the prepared update covers, placed in the middle so the walk reaches it with keys
    # both before and after it still to return.
    blocked_index = nrows // 2

    def keys(self):
        return [f'key{i:04d}' for i in range(self.nrows)]

    # The walk tracks a position in each constituent, and only consults the one it is not
    # currently returning from once that one is also positioned. Interleaving the two constituents'
    # keys keeps both positioned across the whole walk.
    def stable_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 0]

    def ingest_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 1]

    # Walk the whole table, tolerating the prepare conflict on the blocked key. The walk is
    # expected to stop there repeatedly until the prepared transaction resolves; resolve it on the
    # first conflict and keep walking, which is what exercises resuming from a blocked position.
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

        # Populate half the keys and make them stable, so a walk reads them from the stable
        # constituent.
        self.write_at(self.uri, {k: 'v-initial' for k in self.stable_keys()}, 10)
        self.set_global_ts(1, 10)

        # A prepared update written before the announcement lands on the stable constituent; one
        # written after it is routed to ingest. A write inside the step-down window commits above
        # the announced timestamp.
        step_down_ts = 15

        # A prepare before the announcement has to sit below it, like every write that precedes a
        # step-down; one after it is inside the window and sits above.
        prepare_ts = 12 if self.prepare_before_step_down else 30

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
        self.set_step_down_ts(step_down_ts)
        self.write_at(self.uri, {k: 'v-initial' for k in self.ingest_keys()}, 20)

        if not self.prepare_before_step_down:
            prepare()

        def resolve():
            prepare_session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(50) +
                ',durable_timestamp=' + self.timestamp_str(50))

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

        # Every key is returned exactly once, in order: the walk resumed from the blocked position
        # without losing or repeating the keys around it.
        expected = keys if self.forward else list(reversed(keys))
        self.assertEqual(seen, expected)

        # Resolve the announced step-down so the table is verifiable at teardown.
        self.complete_step_down(step_down_ts)

if __name__ == '__main__':
    wttest.run()
