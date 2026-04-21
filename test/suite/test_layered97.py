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
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered97.py
#
# Regression test for a follower's write path on a layered cursor. An
# insert or update on a cursor configured with overwrite=true (the default)
# does not need to consult the stable constituent, so it should only open
# the ingest cursor. With overwrite=false, the write path does need to look
# the key up in both constituents and must therefore open the stable cursor.
#
# The test observes this through the connection-level open-cursor count:
# an overwrite=true insert or update opens exactly one new cursor (ingest),
# while an overwrite=false insert opens at least two (ingest and stable).

@disagg_test_class
class test_layered97(wttest.WiredTigerTestCase):

    table_name = 'test_layered97'
    uri = 'layered:' + table_name

    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'

    disagg_storages = gen_disagg_storages('test_layered97', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def get_conn_stat(self, session, stat_id):
        stat_cursor = session.open_cursor('statistics:', None, None)
        val = stat_cursor[stat_id][2]
        stat_cursor.close()
        return val

    # Perform an operation while measuring how many cursor opens it triggered
    # on the follower connection. The statistics cursor itself is opened and
    # closed around the operation, so its own lifetime does not contribute to
    # the delta.
    def measure_cursor_opens(self, op):
        before = self.get_conn_stat(self.session_follow, stat.conn.cursor_open_count)
        op()
        after = self.get_conn_stat(self.session_follow, stat.conn.cursor_open_count)
        return after - before

    # Seed the leader with a key, checkpoint, and let the follower ingest the
    # checkpoint so that the stable btree exists on the follower.
    def seed_leader_and_advance_follower(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['seed'] = 'seed-val'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    # --------------------------------------------------------------------------
    # An insert on a follower with overwrite=true (the default) should open
    # the ingest cursor only, leaving the stable constituent untouched.
    # --------------------------------------------------------------------------
    def test_follower_insert_overwrite_does_not_open_stable(self):
        self.seed_leader_and_advance_follower()

        cursor = self.session_follow.open_cursor(self.uri)

        def do_insert():
            self.session_follow.begin_transaction()
            cursor['k1'] = 'v1'
            self.session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(2))

        delta = self.measure_cursor_opens(do_insert)
        self.assertEqual(delta, 1,
            "overwrite=true insert on a follower opened {} cursors, "
            "expected 1 (ingest only); delta > 1 means the stable cursor "
            "was opened unnecessarily".format(delta))

        cursor.close()

    # --------------------------------------------------------------------------
    # An update on a follower with overwrite=true (the default) should open
    # the ingest cursor only, leaving the stable constituent untouched.
    # --------------------------------------------------------------------------
    def test_follower_update_overwrite_does_not_open_stable(self):
        self.seed_leader_and_advance_follower()

        # Prime the ingest table with the key we intend to update. This uses
        # a dedicated cursor that we close before measuring the update. The
        # priming insert also goes through the overwrite=true path, so it
        # does not open stable either -- but we still close the cursor to
        # ensure the measured update() stands alone.
        primer = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        primer['k1'] = 'v1'
        self.session_follow.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(2))
        primer.close()

        cursor = self.session_follow.open_cursor(self.uri)

        def do_update():
            self.session_follow.begin_transaction()
            cursor.set_key('k1')
            cursor.set_value('v2')
            self.assertEqual(cursor.update(), 0)
            self.session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(3))

        delta = self.measure_cursor_opens(do_update)
        self.assertEqual(delta, 1,
            "overwrite=true update on a follower opened {} cursors, "
            "expected 1 (ingest only); delta > 1 means the stable cursor "
            "was opened unnecessarily".format(delta))

        cursor.close()

    # --------------------------------------------------------------------------
    # Sanity check for the opposite case: an insert on a follower with
    # overwrite=false must look the key up across both constituents, so the
    # stable cursor is expected to be opened. This guards against a
    # regression that silently drops the stable-side lookup.
    # --------------------------------------------------------------------------
    def test_follower_insert_no_overwrite_opens_stable(self):
        self.seed_leader_and_advance_follower()

        cursor = self.session_follow.open_cursor(self.uri, None, 'overwrite=false')

        def do_insert():
            self.session_follow.begin_transaction()
            cursor.set_key('k1')
            cursor.set_value('v1')
            self.assertEqual(cursor.insert(), 0)
            self.session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(2))

        delta = self.measure_cursor_opens(do_insert)
        self.assertGreaterEqual(delta, 2,
            "overwrite=false insert on a follower opened {} cursors, "
            "expected at least 2 (ingest + stable)".format(delta))

        cursor.close()

    # --------------------------------------------------------------------------
    # Sanity check mirror of the update case: an update on a follower with
    # overwrite=false must look the key up across both constituents before
    # applying the update, so the stable cursor is expected to be opened.
    # The target key lives in the stable constituent (from the ingested
    # checkpoint), so the lookup genuinely has to consult stable.
    # --------------------------------------------------------------------------
    def test_follower_update_no_overwrite_opens_stable(self):
        self.seed_leader_and_advance_follower()

        cursor = self.session_follow.open_cursor(self.uri, None, 'overwrite=false')

        def do_update():
            self.session_follow.begin_transaction()
            cursor.set_key('seed')
            cursor.set_value('seed-val-2')
            self.assertEqual(cursor.update(), 0)
            self.session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(2))

        delta = self.measure_cursor_opens(do_update)
        self.assertGreaterEqual(delta, 2,
            "overwrite=false update on a follower opened {} cursors, "
            "expected at least 2 (ingest + stable)".format(delta))

        cursor.close()
