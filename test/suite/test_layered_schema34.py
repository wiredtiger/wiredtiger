#!/usr/bin/env python
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
#
# test_layered_schema34.py
#    A create published on a follower has no stable constituent until the node steps up. The
#    step-up rebuilds the constituent from the already-published queue entry, and the next
#    checkpoint covering the publish epoch includes the table.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema34(
  LayeredStepdownMixin, wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    table_config = 'key_format=S,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def uri(self, name):
        """Return a distinct layered table URI within this test."""
        return f'layered:{self.test_name}_{name}'

    def test_stepup_rebuilds_published_pending_create(self):
        """
        A follower's published create stays pending in its queue with no stable constituent.
        Step-up rebuilds the constituent from the published entry, and a checkpoint covering the
        publish epoch includes the table with its rows.
        """
        uri = self.uri('published_pending')
        rows = {'k1': 'stepup', 'k2': 'stepup'}

        # A leader checkpoint gives the follower something to pick up.
        self.set_stable_epoch(10)
        self.leader_checkpoint(1)

        conn_follow, session_follow = self.open_follower_epoch(10)

        # The follower's create is published but stays queued: a follower holds no stable
        # constituent, so nothing reaches shared metadata yet.
        session_follow.create(uri, self.table_config)
        self.publish(uri, 20, session=session_follow)
        self.assertFalse(self.uri_stable_exists(conn_follow, uri))
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, uri))

        # Swap roles. Step-up rebuilds the stable constituent from the published pending create,
        # which stays unpublished until a checkpoint covers its epoch.
        self.step_down()
        self.step_up(conn_follow)
        self.assert_table_state(conn_follow, uri, True, False, False)

        # Rows written before the covering checkpoint travel with the publish.
        cursor = session_follow.open_cursor(uri)
        session_follow.begin_transaction()
        for k, v in rows.items():
            cursor[k] = v
        session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(25))
        cursor.close()

        # A checkpoint covering the publish epoch includes the table.
        self.set_stable_epoch(20, conn_follow)
        self.leader_checkpoint(30, conn_follow, session_follow)
        self.assert_table_state(conn_follow, uri, True, True, True)
        self.assertEqual(self.read_kvs_at(uri, 30, session_follow), rows)

        self.close_follower(conn_follow, session_follow)
