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
from wtscenario import make_scenarios

# The checkpoint LSN installed with the checkpoint metadata reaches the page log when the node
# abandons its incomplete checkpoint on step-up: the page log keeps the installed checkpoint and
# drops the records written after it.
@disagg_test_class
class test_layered_stepup19(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_config = 'create,statistics=(all),disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def open_leader(self):
        return self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' +
            'statistics=(all),disaggregated=(role="leader")')

    def insert(self, session, prefix, ts):
        session.begin_transaction()
        cursor = session.open_cursor(self.uri)
        for i in range(100):
            cursor[str(i)] = prefix + str(i)
        cursor.close()
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

    def complete_checkpoint_lsn(self, conn):
        return self.disagg_get_complete_checkpoint_ext(conn)[0]

    def put_page(self, handle, session, page_id, payload):
        args = wiredtiger.PageLogPutArgs()
        args.flags = 0
        args.base_lsn = 0
        args.backlink_lsn = 0
        handle.plh_put(session, page_id, 0, args, bytes(payload, 'utf-8'))
        return args.lsn

    def test_installed_checkpoint_lsn(self):
        conn2 = self.open_leader()
        session2 = conn2.open_session('')
        session2.create(self.uri, 'key_format=S,value_format=S')

        page_log2 = conn2.get_page_log('palite')
        handle2 = page_log2.pl_open_handle(session2, 1000)

        # A page log record below the checkpoint, and one written after it as part of an
        # incomplete checkpoint: no completion follows it.
        below_lsn = self.put_page(handle2, session2, 1, 'below')
        self.insert(session2, 'v1-', 10)
        conn2.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        session2.checkpoint()
        meta1 = self.disagg_get_complete_checkpoint_meta(conn2)
        lsn1 = self.complete_checkpoint_lsn(conn2)
        self.assertGreater(lsn1, below_lsn)
        above_lsn = self.put_page(handle2, session2, 2, 'above')
        self.assertGreater(above_lsn, lsn1)
        self.assertEqual(sorted(handle2.plh_get_page_ids(session2, above_lsn)), [1, 2])

        # The main connection installs the completed checkpoint and continues from it.
        self.conn.reconfigure(
            f'disaggregated=(checkpoint_meta="{meta1}",checkpoint_lsn={lsn1})')
        self.assertEqual(self.complete_checkpoint_lsn(self.conn), lsn1)

        # The new leader abandons above the installed checkpoint: the incomplete record is
        # discarded and the record below the checkpoint stays.
        conn2.reconfigure('disaggregated=(role="follower")')
        conn2.close()
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.assertEqual(self.complete_checkpoint_lsn(self.conn), lsn1)
        page_log = self.conn.get_page_log('palite')
        handle = page_log.pl_open_handle(self.session, 1000)
        self.assertEqual(handle.plh_get_page_ids(self.session, above_lsn), [1])
