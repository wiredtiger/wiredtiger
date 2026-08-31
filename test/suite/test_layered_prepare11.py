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

# A transaction prepared before the step-down timestamp is set, but resolving after it,
# straddles the boundary. Its write already sits in the stable constituent under pre-boundary
# routing, and unlike an ordinary in-flight writer it cannot be rolled back to force a retry into
# ingest once prepared. This is resolved by duplicating the still-prepared update onto ingest
# before the transaction resolves.
@disagg_test_class
class test_layered_prepare11(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    # preserve_prepared is required for the rollback-straddler scenario below: rollback_timestamp
    # (the only signal that tells a rolled-back prepared straddler apart from an ordinary one) is
    # only ever set, and only ever checked against the step-down boundary, under this config.
    conn_base_config = 'statistics=(all),preserve_prepared=true,precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__
    uri = f'layered:{test_name}'

    # A prepared commit straddling the boundary must succeed (never rolled back: the coordinator
    # already committed to it at prepare) and land in ingest, not in the step-down checkpoint.
    def test_prepared_commit_straddler_survives_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddler'] = 'straddler-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))

        self.set_step_down_ts(11)

        # Must not raise WT_ROLLBACK: a prepared transaction can never be rejected at commit.
        self.session.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(12) + ',durable_timestamp=' + self.timestamp_str(12))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 20), {'straddler': 'straddler-value'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 20), {'straddler'})

        # Complete the step-down: no prepares are outstanding any more, so this proceeds normally.
        self.complete_step_down(11)

        # The value survives the role change, still served from ingest.
        self.assertEqual(self.read_kvs_at(self.uri, 20), {'straddler': 'straddler-value'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 20), {'straddler'})

        # It was excluded from the step-down checkpoint itself: that checkpoint is pinned at the
        # boundary (11), strictly below the straddler's commit timestamp (12).
        if self.stable_is_checkpointed(self.conn, self.uri):
            self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(self.uri), 20), set())

    # A prepared rollback straddling the boundary must not resurface as a phantom pending prepare
    # if this node becomes leader again: the rollback is recorded on ingest too.
    def test_prepared_rollback_straddler_does_not_resurface_on_step_up(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['straddler'] = 'straddler-value'
        self.session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(10) + ',prepared_id=' + self.prepared_id_str(1))

        self.set_step_down_ts(11)
        # rollback_timestamp above the boundary is what marks this a straddler needing relocation,
        # the same role durable_timestamp plays for a straddling commit.
        self.session.rollback_transaction('rollback_timestamp=' + self.timestamp_str(12))
        cursor.close()

        # Rolled back: never visible, on either side of the boundary.
        self.assertEqual(self.read_kvs_at(self.uri, 20), {})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 20), set())

        self.complete_step_down(11)
        self.assertEqual(self.read_kvs_at(self.uri, 20), {})

        # Step back up and confirm no phantom prepared transaction was reconstructed from a stale
        # on-disk prepared cell in the step-down checkpoint.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.read_kvs_at(self.uri, 20), {})
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        prepared_discovered = stat_cursor[wiredtiger.stat.conn.txn_prepared_updates][2]
        stat_cursor.close()
        self.assertEqual(prepared_discovered, 0)

if __name__ == '__main__':
    wttest.run()
