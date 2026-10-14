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

# A walk can block on a prepared update on each constituent in turn, at two different points along
# the same walk, with the two prepared transactions resolved independently and in either order. The
# walk must recover cleanly from each block on its own, switching which constituent leads as the
# blocking key on one side is passed, without conflating the two conflicts or skipping a key while
# a block is being resolved on the other side.
@disagg_test_class
class test_layered_async_stepdown23(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages(disagg_only=True)

    uri = 'layered:test_layered_async_stepdown23'

    def test_two_independent_blocks_on_opposite_constituents(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # key1 and key5 (stable) frame the walk. key3, prepared on stable, blocks the walk once it
        # passes key1: this prepare begins and prepares before the step-down timestamp is announced,
        # so it lands on stable, like every write that precedes the announcement.
        self.write_at(self.uri, {'key1': 'v', 'key5': 'v'}, 10)
        self.set_global_ts(1, 10)

        stable_prep_session = self.conn.open_session()
        stable_prep_cursor = stable_prep_session.open_cursor(self.uri, None, None)
        stable_prep_session.begin_transaction()
        stable_prep_cursor['key3'] = 'v'
        stable_prep_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # key2a lands on ingest once the step-down window opens, giving the walk a second key to
        # return between the two blocks. key2b, prepared on ingest, blocks the walk a second time
        # once it passes key2a; key4 lands on ingest after that.
        self.set_step_down_ts(15)
        self.write_at(self.uri, {'key2a': 'v'}, 18)

        ingest_prep_session = self.conn.open_session()
        ingest_prep_cursor = ingest_prep_session.open_cursor(self.uri, None, None)
        ingest_prep_session.begin_transaction()
        ingest_prep_cursor['key2b'] = 'v'
        ingest_prep_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(22))

        self.write_at(self.uri, {'key4': 'v'}, 25)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))

        seen = []
        conflicts = 0
        stable_resolved = ingest_resolved = False

        while True:
            try:
                if cursor.next() != 0:
                    break
            except wiredtiger.WiredTigerError as e:
                self.assertIn(
                    wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT), str(e))
                conflicts += 1
                self.assertLess(conflicts, 10, 'the walk never resumed past a blocking key')
                # Resolve the stable-side block first, so the walk must recover from it and reach
                # the ingest-side block afterward rather than only ever seeing one at a time.
                if not stable_resolved and len(seen) >= 1:
                    stable_prep_session.rollback_transaction()
                    stable_prep_cursor.close()
                    stable_prep_session.close()
                    stable_resolved = True
                elif not ingest_resolved:
                    ingest_prep_session.rollback_transaction()
                    ingest_prep_cursor.close()
                    ingest_prep_session.close()
                    ingest_resolved = True
                continue
            seen.append(cursor.get_key())

        self.session.rollback_transaction()
        cursor.close()
        if not stable_resolved:
            stable_prep_session.rollback_transaction()
            stable_prep_cursor.close()
            stable_prep_session.close()
        if not ingest_resolved:
            ingest_prep_session.rollback_transaction()
            ingest_prep_cursor.close()
            ingest_prep_session.close()

        # Both prepared transactions are rolled back, so neither key3 nor key2b ever existed as a
        # visible value: every other key is returned exactly once, in order.
        self.assertEqual(conflicts, 2, 'expected exactly one block per constituent')
        self.assertEqual(seen, ['key1', 'key2a', 'key4', 'key5'])

if __name__ == '__main__':
    wttest.run()
