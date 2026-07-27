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
from wiredtiger import stat
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown04.py
#    Operational surfaces: schema ops, cached cursors, cursor configs, isolation levels.
@disagg_test_class
class test_layered_async_stepdown04(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:stepdown_ops'

    # The connection-wide count of cursors reused from the session cursor cache.
    def cursor_reopen_count(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        count = stat_cursor[stat.conn.cursor_reopen][2]
        stat_cursor.close()
        return count

    # Table created while armed: writes route to ingest, stable stays empty.
    def test_create_while_armed(self):
        self.set_global_ts(1, 1)
        self.arm(20)

        uri = 'layered:armed_create'
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'k1': 'v', 'k2': 'v'}, 30)

        self.assertEqual(self.read_keys_at(self.ingest_uri(uri), 40), {'k1', 'k2'})
        self.assertEqual(self.read_keys_at(self.stable_uri(uri), 40), set())
        self.assertEqual(self.read_keys_at(uri, 40), {'k1', 'k2'})

    # A table with pre-arm stable content can be dropped while armed.
    def test_drop_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v'}, 10)

        self.arm(20)
        self.dropUntilSuccess(self.session, self.uri)

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.uri, None, None))

    # Cached-cursor reuse across arm picks up armed routing; writes go to ingest.
    def test_cached_cursor_reuse_across_arm(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'stable'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        self.arm(20)

        # Prove the reopen is served from the session cursor cache.
        reopen_count = self.cursor_reopen_count()
        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertGreater(self.cursor_reopen_count(), reopen_count,
            'the reopen must be served from the session cursor cache')

        self.session.begin_transaction()
        cursor['k2'] = 'ingest'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k2'})
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 40), {'k1'})
        self.assertEqual(self.read_keys_at(self.uri, 40), {'k1', 'k2'})

    # A cursor closed before the demotion and reopened afterwards serves the surviving content.
    def test_cached_cursor_reuse_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'pre': 'stable'}, 10)

        self.arm(20)
        self.write_at(self.uri, {'post': 'ingest'}, 30)

        self.complete_step_down(20)

        # Prove the reopen is served from the session cursor cache.
        reopen_count = self.cursor_reopen_count()
        cursor = self.session.open_cursor(self.uri, None, None)
        self.assertGreater(self.cursor_reopen_count(), reopen_count,
            'the reopen must be served from the session cursor cache')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        seen = set()
        while cursor.next() == 0:
            seen.add(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        self.assertEqual(seen, {'pre', 'post'},
            'all content must be readable through a reopened cursor after the step-down')

    # overwrite=false update/remove consult the merged view; the writes land in ingest.
    def test_overwrite_false_ops_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base', 'k2': 'base'}, 10)

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, "overwrite=false")

        # Update of a stable key: found in the merged view, written to ingest.
        self.session.begin_transaction()
        cursor.set_key('k1')
        cursor.set_value('updated')
        self.assertEqual(cursor.update(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        # Remove of a stable key: a tombstone routed to ingest.
        self.session.begin_transaction()
        cursor.set_key('k2')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))

        # Update and remove of a missing key fail across the merged view.
        self.session.begin_transaction()
        cursor.set_key('missing')
        cursor.set_value('v')
        self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        cursor.set_key('missing')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'updated'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'k1', 'k2'})
        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 40),
            {'k1': 'base', 'k2': 'base'})

    # Bounds set before the arm apply to keys from both constituents.
    def test_bounded_cursor_across_arm(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        cursor = self.session.open_cursor(self.uri, None, None)
        cursor.set_key('b')
        self.assertEqual(cursor.bound('action=set,bound=lower'), 0)
        cursor.set_key('e')
        self.assertEqual(cursor.bound('action=set,bound=upper'), 0)

        self.arm(20)
        self.write_at(self.uri, {'a': 'i', 'c': 'i', 'e': 'i'}, 30)

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        seen = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, ['b', 'c', 'd', 'e'],
            'a bounded scan across the arm must respect bounds on both constituents')

    # A readonly cursor reads the merged view while armed and rejects writes.
    def test_readonly_cursor_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'stable'}, 10)

        self.arm(20)
        self.write_at(self.uri, {'k2': 'ingest'}, 30)

        cursor = self.session.open_cursor(self.uri, None, 'readonly=true')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        self.assertEqual(cursor['k1'], 'stable')
        self.assertEqual(cursor['k2'], 'ingest')
        self.session.rollback_transaction()

        self.session.begin_transaction()
        cursor.set_key('k3')
        cursor.set_value('v')
        with self.expectedStderrPattern('Unsupported cursor operation'):
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.insert())
        self.session.rollback_transaction()
        cursor.close()

    # next_random while armed samples the post-arm ingest content.
    def test_next_random_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.arm(20)

        keys = {f'k{i:02d}' for i in range(10)}
        self.write_at(self.uri, {k: 'i' for k in keys}, 30)

        cursor = self.session.open_cursor(self.uri, None, 'next_random=true')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        for _ in range(10):
            self.assertEqual(cursor.next(), 0)
            self.assertIn(cursor.get_key(), keys)
        self.session.rollback_transaction()
        cursor.close()

    # A post-arm reserve conflicts with concurrent writers and leaves no content behind.
    def test_reserve_while_armed(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'stable'}, 10)

        self.arm(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k1')
        self.assertEqual(cursor.reserve(), 0)

        # A concurrent writer conflicts with the reservation.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur.set_key('k1')
        wcur.set_value('other')
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: wcur.update(),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_ROLLBACK))
        wsession.rollback_transaction()
        wcur.close()
        wsession.close()

        # The reserve-only commit leaves no content behind in either constituent.
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'stable'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())
