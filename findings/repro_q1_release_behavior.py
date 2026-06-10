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
#
# test_q1_release.py
#
# Empirically determine what a RELEASE build (WT_ASSERT compiled out) does for a
# cross-transaction positional remove on a disaggregated follower layered cursor.
# See __clayered_remove_follower in src/cursor/cur_layered.c.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_q1_release(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),'
    uri = 'layered:test_q1_release'

    disagg_storages = gen_disagg_storages('test_q1_release', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def fresh_state(self, session, label):
        # Report whether key 1 exists, via both a scan and a point search, with a fresh cursor.
        scan = []
        c = session.open_cursor(self.uri)
        while c.next() != wiredtiger.WT_NOTFOUND:
            scan.append((c.get_key(), c.get_value()))
        c.close()

        s = session.open_cursor(self.uri)
        s.set_key(1)
        sret = s.search()
        sval = None if sret != 0 else s.get_value()
        s.close()

        self.pr('STATE[%s]: scan=%r  search(1)=%s value=%r' %
                (label, scan, 'NOTFOUND' if sret == wiredtiger.WT_NOTFOUND
                 else ('0' if sret == 0 else sret), sval))
        return scan, sret, sval

    def put_key1_follower(self, ts):
        # Mirror an insert of key 1 into the follower's ingest with a commit timestamp.
        c = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        c[1] = 'v1'
        self.session_follow.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(ts))
        c.close()

    def test_q1_release(self):
        self.setup_follower()

        table_config = 'key_format=i,value_format=S'
        self.session.create(self.uri, table_config)
        self.session_follow.create(self.uri, table_config)

        # Advance oldest/stable so commit timestamps are valid on the follower.
        self.conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        # ---- main experiment: cross-transaction positional remove on follower ----
        self.pr('=== Q1: cross-transaction positional remove (RELEASE) ===')
        self.put_key1_follower(ts=10)

        c = self.session_follow.open_cursor(self.uri)

        # Position with an autocommit search: this commits the positioning txn.
        c.set_key(1)
        self.assertEqual(c.search(), 0)
        self.assertEqual(c.get_value(), 'v1')

        # New transaction; positional remove (NO set_key); commit with a timestamp.
        self.session_follow.begin_transaction()
        remove_ret = None
        remove_exc = None
        try:
            remove_ret = c.remove()
        except Exception as e:
            remove_exc = e
        self.pr('Q1 remove() ret=%r exc=%r' % (remove_ret, remove_exc))

        commit_ok = None
        commit_exc = None
        try:
            self.session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(20))
            commit_ok = True
        except Exception as e:
            commit_ok = False
            commit_exc = e
            try:
                self.session_follow.rollback_transaction()
            except Exception:
                pass
        self.pr('Q1 commit_ok=%r commit_exc=%r' % (commit_ok, commit_exc))
        c.close()

        q1_state = self.fresh_state(self.session_follow, 'Q1 after cross-txn remove')

        # ---- Control A: same-transaction positional remove ----
        self.pr('=== Control A: same-txn positional remove ===')
        self.put_key1_follower(ts=30)
        ca = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        ca.set_key(1)
        self.assertEqual(ca.search(), 0)
        ca_remove_ret = ca.remove()
        self.session_follow.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(40))
        ca.close()
        self.pr('Control A remove() ret=%r' % (ca_remove_ret,))
        ca_state = self.fresh_state(self.session_follow, 'Control A after same-txn remove')

        # ---- Control B (Path A): autocommit positional remove, no explicit txn ----
        self.pr('=== Control B: autocommit positional remove (Path A) ===')
        self.put_key1_follower(ts=50)
        cb = self.session_follow.open_cursor(self.uri)
        cb.set_key(1)
        self.assertEqual(cb.search(), 0)
        cb_remove_ret = None
        cb_remove_exc = None
        try:
            cb_remove_ret = cb.remove()
        except Exception as e:
            cb_remove_exc = e
        self.pr('Control B remove() ret=%r exc=%r' % (cb_remove_ret, cb_remove_exc))
        cb.close()
        cb_state = self.fresh_state(self.session_follow, 'Control B after autocommit remove')

        # ---- Emit a single machine-readable summary line. ----
        def fmt(scan, sret, sval):
            return 'scan=%r search=%s sval=%r' % (
                scan,
                'NOTFOUND' if sret == wiredtiger.WT_NOTFOUND else sret,
                sval)
        self.pr('Q1_SUMMARY ::: '
                'Q1[remove_ret=%r remove_exc=%r commit_ok=%r commit_exc=%r %s] ||| '
                'CtrlA[remove_ret=%r %s] ||| '
                'CtrlB[remove_ret=%r remove_exc=%r %s]' % (
                    remove_ret, str(remove_exc), commit_ok, str(commit_exc),
                    fmt(*q1_state),
                    ca_remove_ret, fmt(*ca_state),
                    cb_remove_ret, str(cb_remove_exc), fmt(*cb_state)))

        # ---- Assertions documenting the empirically determined RELEASE behavior. ----
        # Q1: cross-txn positional remove with a commit timestamp is a CLEAN, CORRECT remove.
        self.assertEqual(remove_ret, 0)
        self.assertIsNone(remove_exc)
        self.assertTrue(commit_ok)
        self.assertEqual(q1_state[0], [])                       # scan empty
        self.assertEqual(q1_state[1], wiredtiger.WT_NOTFOUND)   # search(1) NOTFOUND
        # Q1 matches the same-txn control exactly.
        self.assertEqual(q1_state[0], ca_state[0])
        self.assertEqual(q1_state[1], ca_state[1])
        # Control B: autocommit remove (no timestamp) surfaces EINVAL, key 1 untouched.
        self.assertIsNotNone(cb_remove_exc)
        self.assertEqual(cb_state[0], [(1, 'v1')])
        self.assertEqual(cb_state[1], 0)

        # Control B's autocommit remove legitimately logs the timestamp-usage rejection to
        # stderr; that is the behavior under test, not a test failure. The output exists now,
        # so the if-exists ignore takes effect.
        self.ignoreStderrPatternIfExists('unexpected timestamp usage|always use timestamps|verbose_dump_txn')

if __name__ == '__main__':
    wttest.run()
