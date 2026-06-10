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

# Minimal repro: on a disaggregated FOLLOWER, a positional remove() issued in a
# NEW transaction (after the positioning search() already committed) is mishandled
# by __clayered_remove_follower (src/cursor/cur_layered.c). "Positioned" is taken
# from the iface KEY_INT flag (which survives the commit), but the code then reads
# the ingest cursor's VALUE_INT/value, which does NOT survive the transaction
# switch -> WT_ASSERT abort (diagnostic build) / stale-value read (release build).
@disagg_test_class
class test_repro_cross_txn_remove(wttest.WiredTigerTestCase):
    uri = 'layered:test_repro_cross_txn_remove'
    conn_base_config = 'create,statistics=(all),'
    disagg_storages = gen_disagg_storages('test_repro_cross_txn_remove', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + ',' + self.conn_base_config + 'disaggregated=(role="leader")'

    def test_cross_txn_positional_remove(self):
        cfg = 'key_format=i,value_format=S'
        self.session.create(self.uri, cfg)
        follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',' + self.conn_base_config + 'disaggregated=(role="follower")')
        s = follow.open_session('')
        s.create(self.uri, cfg)

        # Put one key into the follower's ingest table.
        c = s.open_cursor(self.uri)
        s.begin_transaction()
        c[1] = 'v1'
        s.commit_transaction('commit_timestamp=' + self.timestamp_str(1))

        # Position on it (this autocommit search commits), then remove it in a
        # SECOND transaction via a positional remove (no set_key).
        c.set_key(1)
        self.assertEqual(c.search(), 0)
        s.begin_transaction()
        c.remove()
        s.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
