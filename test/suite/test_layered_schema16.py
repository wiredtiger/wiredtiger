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

# A table created and dropped at or below the stable schema epoch and recreated above it stays
# unpublished until the stable schema epoch reaches the recreate. Writing stable data to the
# recreate and checkpointing before then is an API violation: the checkpoint would include the
# stable constituent of an unpublished table. Publication is decided from the table's latest
# create/remove, so the stale earlier create no longer publishes the recreate, and the checkpoint
# panics with the protocol violation instead of silently leaking the table into shared metadata.

import os
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema16(wttest.WiredTigerTestCase, suite_subprocess, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    uri = f'layered:{test_name}'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def subprocess_recreate_above_stable_epoch_panics(self):
        """
        Subprocess body: run the create/drop/recreate-above-epoch sequence and checkpoint the
        recreate's stable data while it is still unpublished. This must panic.
        """
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(1)

        # Create and publish the table at epoch 2, checkpoint so it is durable.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 2)
        self.leader_checkpoint(2)

        # Drop and publish the drop at epoch 3, checkpoint so the removal is durable.
        self.session.drop(self.uri)
        self.publish(self.uri, 3)
        self.leader_checkpoint(3)

        # Recreate at epoch 9 (above the stable schema epoch set below) and write a row.
        self.session.create(self.uri, self.table_config)
        self.publish(self.uri, 9)
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[1] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        # Advance the stable schema epoch to 5, between the drop (3) and the recreate (9), then
        # checkpoint. The recreate is still unpublished at epoch 5, so checkpointing its stable
        # data is a protocol violation and panics.
        self.set_stable_epoch(5)
        self.leader_checkpoint(10)

    def test_recreate_above_stable_epoch_panics(self):
        """
        Checkpointing stable data for a recreate that is still unpublished at the stable schema
        epoch panics rather than silently leaking the table into shared metadata.
        """
        # The parent connection does no work here, but precise checkpoint requires a stable
        # timestamp for its shutdown checkpoint to succeed.
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))

        [returncode, home] = self.run_subprocess_function(
            'SUBPROCESS_recreate_above_stable_epoch_panics',
            f'{self.test_name}.{self.test_name}.subprocess_recreate_above_stable_epoch_panics',
            silent=True)
        self.assertNotEqual(returncode, 0)
        self.check_file_contains(os.path.join(home, 'stderr.txt'),
            'stable data checkpointed for unpublished table')
