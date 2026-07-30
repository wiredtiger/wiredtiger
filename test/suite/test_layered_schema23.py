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

# Test that a legacy (no schema epoch) step-down drops the local stable
# constituents that the shared metadata does not cover.
#
# Without schema epochs the shared metadata queue cannot mark which local stable
# tables a checkpoint covers, so step-down reconciles against the shared metadata
# directly, mirroring the legacy step-up that creates missing stable tables. A
# table created as leader and checkpointed is published and kept; one created
# without a covering checkpoint is uncovered, so its stable constituent must be
# dropped when the node becomes a follower.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema23(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    uri_uncovered = f'layered:{test_name}_uncovered'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def stable_uri(self, uri):
        """Return the stable constituent URI for a layered table URI."""
        return 'file:' + uri.split(':', 1)[1] + '.wt_stable'

    def test_legacy_step_down_drops_uncovered_stable(self):
        # Legacy leader: no schema epoch is ever set.
        self.session.create(self.uri, self.table_config)
        cursor = self.session.open_cursor(self.uri)
        cursor[1] = 'published'
        cursor.close()

        # A checkpoint publishes the first table to the shared metadata.
        self.session.checkpoint()
        self.assertTrue(self.uri_in_shared_metadata(self.conn, self.uri))

        # A second table created without a covering checkpoint stays local-only.
        self.session.create(self.uri_uncovered, self.table_config)
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri_uncovered))
        self.assertFalse(self.uri_in_shared_metadata(self.conn, self.uri_uncovered))

        # Step down: the uncovered table's stable constituent is dropped, while the
        # published table's stable constituent is kept.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertFalse(self.uri_in_local_metadata(self.conn, self.uri_uncovered))
        self.assertTrue(self.uri_in_local_metadata(self.conn, self.uri))
