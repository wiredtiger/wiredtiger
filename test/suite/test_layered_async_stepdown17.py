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

# A walk blocked on the stable constituent's own prepared conflict survives into a role change:
# the reader is left mid-walk (not reset) while the leader steps down to follower. Only write
# cursors are required to be unpositioned across a step-down. The reopened stable cursor loses the
# blocked position, while the ingest constituent -- the walk's alternate -- can still be genuinely
# positioned. The walk must recover from that mismatch on the next call rather than reuse the stale
# pairing: the step-down checkpoint never carries a prepared, uncommitted update, so the restarted
# walk finds nothing blocking key0020 and returns every key in one pass.
@disagg_test_class
class test_layered_async_stepdown17(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = disagg_storages

    uri = 'layered:test_layered_async_stepdown17'

    nrows = 40
    blocked_index = nrows // 2

    def keys(self):
        return [f'key{i:04d}' for i in range(self.nrows)]

    # Interleave so the walk toggles between constituents, keeping ingest genuinely positioned
    # once the walk reaches (and blocks on) the stable-side prepared key.
    def stable_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 0]

    def ingest_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 1]

    def write_at(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def test_role_change_while_stable_blocked(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = self.keys()
        blocked_key = keys[self.blocked_index]

        # Stable content, prepared conflict lands here (even index -> stable key).
        self.write_at(self.uri, {k: 'v-initial' for k in self.stable_keys()}, 10)
        self.set_global_ts(1, 10)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor[blocked_key] = 'v-prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # Announce the step-down, then give ingest its own genuinely positioned content.
        self.set_step_down_ts(15)
        self.write_at(self.uri, {k: 'v-initial' for k in self.ingest_keys()}, 20)

        # Walk until the prepared conflict blocks it. The current cursor at that point is stable,
        # blocked on the prepared key, while ingest is independently positioned as the alternate.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        seen = []
        hit_conflict = False
        while not hit_conflict:
            try:
                self.assertEqual(cursor.next(), 0)
            except wiredtiger.WiredTigerError as e:
                self.assertIn(
                    wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT), str(e))
                hit_conflict = True
                continue
            seen.append(cursor.get_key())

        # A role change now runs while the reader is still mid-walk, blocked on the stable-side
        # conflict.
        self.complete_step_down(15)

        # Resume the walk. The role change discarded the blocked position, so the walk restarts
        # from the first key rather than resuming where it left off.
        seen = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, keys)

        # Resolving a prepared update whose btree was outdated by the role change is a separate,
        # already-known issue (EBUSY reopening the now-outdated stable dhandle), orthogonal to the
        # walk recovery this test targets; tolerate it rather than assert on it here.
        try:
            prepare_session.rollback_transaction()
        except wiredtiger.WiredTigerError as e:
            if 'busy' not in str(e).lower():
                raise
        prepare_cursor.close()
        prepare_session.close()

if __name__ == '__main__':
    wttest.run()
