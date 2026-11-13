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
from wtscenario import make_scenarios

# test_verify_disagg09.py
#    Verify a follower does not report corruption for a kept key whose start time the leader
# cleared.
#
# The leader clears the start time of a key once it is visible to every reader, so the checkpoint's
# aggregate no longer covers it. The follower rebuilds the page from the older image, where the key
# still has its start time, which can then exceed the aggregate's newest durable start.
@disagg_test_class
class test_verify_disagg09(wttest.WiredTigerTestCase):
    test_name = __qualname__
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_config = 'disaggregated=(role="leader"),page_delta=(delta_pct=100,leaf_page_delta=true)'
    conn_config_follower = 'disaggregated=(role="follower"),statistics=(all)'

    table_cfg = 'key_format=S,value_format=S,block_manager=disagg'
    uri = f'layered:{test_name}'

    def put(self, cursor, key, t):
        self.session.begin_transaction()
        cursor[key] = 'a' * 200
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(t))

    def remove(self, cursor, key, t):
        self.session.begin_transaction()
        cursor.set_key(key)
        cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(t))

    def test_durable_start_after(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, self.table_cfg)
        cursor = self.session.open_cursor(self.uri, None, None)

        # Key 'k' starts at 3. Key 'm' starts at 2 inside a later transaction, and a long-running
        # reader stops its start from being cleared, so the aggregate's newest durable start is 2.
        self.put(cursor, 'k', 3)
        self.reader = self.conn.open_session('')
        self.reader.begin_transaction()
        self.put(cursor, 'm', 2)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(3))
        self.session.checkpoint()

        # Deleting 'm' dirties the leaf. With oldest at 4 the leader clears the start of 'k'.
        self.remove(cursor, 'm', 5)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(4) +
                                ',stable_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()
        cursor.close()

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
                                           self.conn_config_follower)
        session_follow = conn_follow.open_session('')
        self.disagg_advance_checkpoint_and_wait(conn_follow)

        self.verifyUntilSuccess(self.session)
        self.verifyUntilSuccess(session_follow)
