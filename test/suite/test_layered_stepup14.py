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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered_stepup14.py
# Step-up must clear the ingest table even when the clearing truncate conflicts with the tree
# changing underneath it and has to retry.
@disagg_test_class
class test_layered_stepup14(wttest.WiredTigerTestCase):
    nitems = 2000

    conn_base_config = ',create,statistics=(all),' \
        + 'timing_stress_for_test=[failpoint_non_transactional_truncate_restart],'

    disagg_storages = gen_disagg_storages(disagg_only = True)

    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_stepup14'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def get_stat(self, stat_id):
        stat_cursor = self.session.open_cursor('statistics:')
        value = stat_cursor[stat_id][2]
        stat_cursor.close()
        return value

    def test_clear_ingest_table_retry(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Populate the ingest table as a follower.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.session = self.conn.open_session('')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[f'key-{i:010d}'] = f'value-{i}'
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()

        # Step up.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # The clear must have recovered from at least one conflict.
        self.assertGreater(
            self.get_stat(stat.conn.disagg_step_up_clear_ingest_fail), 0)

        # All content must survive the step-up.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.assertEqual(cursor[f'key-{i:010d}'], f'value-{i}')
        cursor.close()
