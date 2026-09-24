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
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_frozen_pin01.py
#    A leader writes pages to the page log after its last checkpoint and demotes. A successor that
#    was never demoted promotes, abandoning everything above that checkpoint. The demoted node must
#    still read all of its commits as a follower, and when it later promotes and checkpoints, a
#    fresh follower must read them from that checkpoint.
@disagg_test_class
class test_layered_frozen_pin01(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_frozen_pin01'
    nrows = 20000
    old_value = 'a' * 200
    new_value = 'b' * 200

    def key(self, i):
        return 'k%08d' % i

    def write(self, keys, value, commit_ts, session=None):
        session = session or self.session
        cursor = session.open_cursor(self.uri, None, None)
        for batch in range(0, len(keys), 500):
            session.begin_transaction()
            for k in keys[batch:batch + 500]:
                cursor[k] = value
            session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def evict_all(self):
        self.session.reconfigure('debug=(release_evict_page=true)')
        cursor = self.session.open_cursor(self.stable_uri(self.uri), None, None)
        self.session.begin_transaction()
        while cursor.next() == 0:
            pass
        self.session.rollback_transaction()
        cursor.close()
        self.session.reconfigure('debug=(release_evict_page=false)')

    def assert_all(self, expected, read_ts, session):
        cursor = session.open_cursor(self.uri, None, None)
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        seen = {}
        while cursor.next() == 0:
            seen[cursor.get_key()] = cursor.get_value()
        session.rollback_transaction()
        cursor.close()
        self.assertEqual(len(seen), len(expected))
        self.assertTrue(seen == expected, 'the demoted node read stale or missing rows')

    def test_foreign_abandon_after_demote(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S,leaf_page_max=4KB')

        # Settle the checkpointed rows onto on-disk leaves, so the update below replaces each leaf
        # in place rather than splitting it.
        keys = [self.key(i) for i in range(self.nrows)]
        self.write(keys, self.old_value, 10)
        self.checkpoint_at(20)
        self.evict_all()

        # Update every row after the checkpoint and evict twice: the first pass writes every leaf
        # to the page log above the checkpoint and keeps a clean image, the second drops it.
        writes = self.connection_stat(stat.conn.rec_page_full_image_leaf) + \
            self.connection_stat(stat.conn.rec_page_delta_leaf)
        self.write(keys, self.new_value, 30)
        self.set_global_ts(30, 30)
        self.evict_all()
        self.evict_all()
        self.assertGreater(self.connection_stat(stat.conn.rec_page_full_image_leaf) +
            self.connection_stat(stat.conn.rec_page_delta_leaf), writes + 100)

        self.demote()
        self.assertGreater(self.connection_stat(stat.conn.disagg_frozen_pages_pinned), 100)

        # Read nothing on the demoted node yet: that would fault in the updated leaves and hide
        # a missing pin. The successor adopts the checkpoint at 20 and promotes, which abandons.
        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        self.promote(conn_b)

        expected = {k: self.new_value for k in keys}
        self.assert_all(expected, 30, self.session)

        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

        # Stepping back up continues this node's lineage; its next checkpoint must not reference
        # the abandoned page versions.
        self.promote()
        self.assertGreater(self.connection_stat(stat.conn.disagg_frozen_pages_rewritten), 100)
        more_keys = [self.key(self.nrows + i) for i in range(100)]
        self.write(more_keys, self.new_value, 50)
        self.checkpoint_at(50)
        expected.update({k: self.new_value for k in more_keys})
        self.assert_all(expected, 50, self.session)

        conn_c = self.open_node('fresh', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assert_all(expected, 50, session_c)
        session_c.close()
        conn_c.close()
