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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# A follower truncate over another transaction's prepared update must surface a write conflict, the
# same as the leader, rather than a prepare conflict or (worse) no conflict at all.
@disagg_test_class
class test_layered_fast_truncate22(wttest.WiredTigerTestCase):
    uri = 'table:test_layered_fast_truncate22'
    table_config = 'key_format=i,value_format=S,block_manager=disagg,type=layered'
    conn_base_config = 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_truncate_over_prepared_update_conflicts(self):
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() +
            ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')

        self.session.create(self.uri, self.table_config)
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(self.uri)
            for k in range(1, 101):
                c[k] = 'v'
            c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Prepare an update on key 45 in the follower's ingest table.
        prep_session = conn_follow.open_session('')
        prep_cursor = prep_session.open_cursor(self.uri)
        prep_session.begin_transaction()
        prep_cursor[45] = 'prepared'
        prep_cursor.close()
        prep_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(300) +
            ',prepared_id=' + self.prepared_id_str(1))

        # A truncate covering the prepared key must roll back, not raise a prepare conflict.
        trunc_session = conn_follow.open_session('')
        c_lo = trunc_session.open_cursor(self.uri)
        c_hi = trunc_session.open_cursor(self.uri)
        c_lo.set_key(30)
        c_hi.set_key(60)
        trunc_session.begin_transaction()
        self.assertRaisesException(
            wiredtiger.WiredTigerError,
            lambda: trunc_session.truncate(None, c_lo, c_hi, None),
            '/conflict between concurrent operations/')
        trunc_session.rollback_transaction()
        c_lo.close()
        c_hi.close()

        prep_session.rollback_transaction()
        conn_follow.close()
