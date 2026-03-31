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

import os, re, sys, wttest
from helper_disagg import disagg_test_class
from wtscenario import make_scenarios


# test_layered79.py
#   Verify that ingest btrees created on a follower are never
#   classified as shared.
@disagg_test_class
class test_layered79(wttest.WiredTigerTestCase):

    uri_base = 'test_layered79'

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(lose_all_my_data=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    ntables = 20

    scenarios = make_scenarios([
        ('layered-bare', dict(prefix='layered:', extra_config='')),
        ('layered-disagg', dict(prefix='layered:', extra_config='block_manager=disagg')),
        ('table-type-layered', dict(prefix='table:', extra_config='block_manager=disagg,type=layered')),
    ])

    def parse_id(self, meta_val):
        """Return the numeric btree ID stored in a metadata value string."""
        m = re.search(r',id=(\d+)', meta_val)
        self.assertTrue(m, f'no id= field in metadata: {meta_val}')
        return int(m.group(1))

    def check_namespace(self, session, uri_base, table_range):
        """
        Iterate the metadata cursor and for every file belonging to uri_base assert:
          - file:<base>_N.wt_stable  - shared namespace  (id is odd)
          - file:<base>_N.wt_ingest  - local namespace   (id is even)
        """
        mc = session.open_cursor('metadata:')
        while mc.next() == 0:
            uri = mc.get_key()
            
            # Skip all WT special tables.
            if not any(uri.startswith(f'file:{uri_base}_{i}') for i in table_range):
                continue

            file_id = self.parse_id(mc.get_value())
            if uri.endswith('.wt_stable'):
                self.assertTrue(file_id % 2 == 1,
                    f'{uri}: id={file_id} should be odd (shared namespace)')
            elif uri.endswith('.wt_ingest'):
                self.assertTrue(file_id % 2 == 0,
                    f'{uri}: id={file_id} should be even (local namespace)')
        mc.close()

    def test_ingest_btree_not_shared(self):
        uris = [f'{self.prefix}{self.uri_base}_{i}' for i in range(self.ntables)]
        create_config = 'key_format=S,value_format=S'
        if self.extra_config:
            create_config += ',' + self.extra_config

        # Create tables on the leader and checkpoint so that shared metadata is
        # published.
        for uri in uris:
            self.session.create(uri, create_config)
        self.session.checkpoint()

        # Leader: .wt_stable IDs must be shared; .wt_ingest must be local.
        self.check_namespace(self.session, self.uri_base, range(self.ntables))

        # Open a follower, passing the current checkpoint so that it initialises
        # next_file_id from the leader's shared metadata.
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()
        conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config +
            f'disaggregated=(role="follower",checkpoint_meta="{checkpoint_meta}")')
        session_follow = conn_follow.open_session('')

        # Create the same layered tables on the follower.
        for uri in uris:
            session_follow.create(uri, create_config)

        # Follower: same namespace invariant must hold. The .wt_stable entries
        # come from shared metadata picked up at open. the .wt_ingest entries should
        # be created locally and have local file IDs.
        self.check_namespace(session_follow, self.uri_base, range(self.ntables))

        session_follow.close()
        conn_follow.close()
