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

# A disaggregated table dropped at or below the stable schema epoch and recreated above it must not
# become durable or reappear after recovery. The recreated table is above the stable schema epoch,
# so it is unpublished: its stable constituent must not reach the shared metadata, and a recovered
# node must not resurrect it. The last durable schema operation at or below the stable epoch is the
# drop, so the table must be absent.
#
# Without the fix, the recreated table's btree is opened without arming the unpublished guard (no
# stable schema epoch was set when it was created), so its stable constituent is checkpointed into
# the shared metadata and recovery resurrects it.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema16(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def simulate_crash_recovery(self):
        """
        Simulate a leader crash and recovery.

        A real leader recovers by reopening with lose_all_my_data=true (see
        test/csuite/schema_disagg_abort), which discards local files and rebuilds
        from the shared checkpoint. Closing with skip_checkpoint mimics a SIGKILL
        by suppressing the shutdown checkpoint, so the last durable state is the
        last explicit checkpoint.
        """
        self.close_conn('debug=(skip_checkpoint=true)')
        self.ignoreStdoutPattern('WT_VERB_METADATA')
        self.open_conn()

        # Recovery with lose_all_my_data resets the timestamps. Set a stable timestamp so the
        # recovered connection can take its shutdown checkpoint (precise checkpoint requires one).
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(10) +
            ',oldest_timestamp=' + self.timestamp_str(1))

    def test_recreate_above_stable_epoch_not_resurrected(self):
        """
        A table dropped at or below the stable schema epoch and recreated above it must stay
        unpublished: its stable constituent must not reach the shared metadata, and it must be
        absent after recovery.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))

        # Create and publish the table at epoch 2, checkpoint so it is durable.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 2)
        self.leader_checkpoint(2)

        # Drop and publish the drop at epoch 3, checkpoint so the removal is durable.
        self.session.drop(self.uri)
        self.publish(self.uri, 3)
        self.leader_checkpoint(3)

        # Recreate at epoch 9 (above the stable epoch set below) and write an unstable row (commit
        # above the checkpoint stable timestamp), so the table stays unpublished.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 9)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        cursor.close()

        # Advance the stable schema epoch to 5, between the drop (3) and the recreate (9).
        self.set_stable_epoch(5)
        self.leader_checkpoint(10)

        # The recreate is above the stable schema epoch, so its stable constituent must not have
        # leaked into the shared metadata.
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri),
            'an unpublished above-epoch table leaked into the shared metadata')

        self.simulate_crash_recovery()

        # The last schema operation at or below the durable epoch (5) is the drop at epoch 3, so the
        # table must be absent. Before the fix it is resurrected.
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri))
