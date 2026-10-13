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

# Creating a missing stable constituent during step-up must preserve the
# table's file settings. Check that verification fails when shared metadata
# indicates those settings were lost.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_disagg_stable_config(wttest.WiredTigerTestCase):
    table = __qualname__
    uri = f"layered:{table}"
    stable_uri = f"file:{table}.wt_stable"
    shared_uri = "file:WiredTigerShared.wt_stable"

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self, role="leader", checkpoint_meta=None):
        disagg_config = f'role="{role}"'
        if checkpoint_meta is not None:
            disagg_config += f',checkpoint_meta="{checkpoint_meta}"'

        return f"{self.extensionsConfig()},create,disaggregated=({disagg_config})"

    def test_persisted_bad_config_fails(self):
        """Check that verification fails with invalid persisted stable configuration."""
        # Begin with a valid stable config.
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(10))
        self.session.checkpoint()

        # Change the stored config directly, bypassing normal table creation.
        with wttest.open_cursor(self.session, self.shared_uri) as c:
            config = c[self.stable_uri]
            self.assertIn("block_manager=disagg", config)
            c[self.stable_uri] = config.replace("block_manager=disagg", "block_manager=default")
        self.session.checkpoint()

        # The follower reports an error when it verifies the invalid stable config.
        meta = self.disagg_get_complete_checkpoint_meta()
        follow_conn = self.wiredtiger_open(
            "follower", self.conn_config(role="follower", checkpoint_meta=meta)
        )
        follow_session = follow_conn.open_session("")
        self.assertRaisesWithMessage(
            wiredtiger.WiredTigerError,
            lambda: follow_session.verify(self.uri),
            f'/Shared stable metadata for "{self.stable_uri}" has '
            'block_manager="default", expected "disagg"/',
        )

    def test_healthy_config_verifies(self):
        """Check that verification succeeds with valid persisted stable configuration."""
        # Begin with a valid stable config.
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(10))
        self.session.checkpoint()

        # The follower verifies successfully.
        meta = self.disagg_get_complete_checkpoint_meta()
        follow_conn = self.wiredtiger_open(
            "follower", self.conn_config(role="follower", checkpoint_meta=meta)
        )
        self.assertEqual(follow_conn.open_session("").verify(self.uri), 0)
