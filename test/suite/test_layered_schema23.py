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
# constituents that no checkpoint has published to the shared metadata.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema23(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def uri(self, name):
        """Return a distinct layered table URI within this test."""
        return f'layered:{self.test_name}_{name}'

    def create_published(self, uri, value=None):
        """Create a table, optionally write a row, and checkpoint so it reaches shared metadata."""
        self.session.create(uri, self.table_config)
        if value is not None:
            cursor = self.session.open_cursor(uri)
            cursor[1] = value
            cursor.close()
        self.session.checkpoint()
        self.assertTrue(self.uri_in_shared_metadata(self.conn, uri))

    def step_down(self):
        self.conn.reconfigure('disaggregated=(role="follower")')

    def test_legacy_step_down_drops_uncovered_stable(self):
        """A published table's stable is kept with its data; an uncovered table's is dropped."""
        published = self.uri('published')
        self.create_published(published, value='published')

        # A second table created without a covering checkpoint stays local-only.
        uncovered = self.uri('uncovered')
        self.session.create(uncovered, self.table_config)
        self.assertFalse(self.uri_in_shared_metadata(self.conn, uncovered))

        self.step_down()
        self.assertFalse(self.uri_stable_exists(self.conn, uncovered))
        self.assertTrue(self.uri_stable_exists(self.conn, published))
        cursor = self.session.open_cursor(published)
        self.assertEqual({k: v for k, v in cursor}, {1: 'published'})
        cursor.close()

    def test_legacy_multiple_uncovered(self):
        """Step-down keeps every published stable and drops every uncovered one."""
        published = [self.uri(f'published{i}') for i in range(3)]
        for uri in published:
            self.create_published(uri, value='v')

        uncovered = [self.uri(f'uncovered{i}') for i in range(3)]
        for uri in uncovered:
            self.session.create(uri, self.table_config)

        self.step_down()
        for uri in published:
            self.assertTrue(self.uri_stable_exists(self.conn, uri))
        for uri in uncovered:
            self.assertFalse(self.uri_stable_exists(self.conn, uri))
            # The ingest half survives the step-down.
            self.assertTrue(self.uri_in_local_metadata(self.conn, uri))
