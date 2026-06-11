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

import re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_stepdown_async01.py
#    POC scenarios for asynchronous (elegant) step-down (FIXME-WT-17785).
#
#    A prepare-to-step-down cutoff timestamp is armed on the leader via
#    conn.set_timestamp('prepare_to_step_down=T'). While armed:
#      - writes whose commit timestamp is already known are routed to the
#        stable constituent (ts <= T) or the ingest constituent (ts > T),
#      - writes whose commit timestamp is not yet known go to both
#        constituents ("double-write") and the losing copy is aborted when
#        the timestamp is assigned at commit,
#      - the step-down checkpoint at stable == T contains exactly the
#        pre-cutoff content; post-cutoff content survives the demote in the
#        ingest table, exactly where a follower would have it.
#
#    Constituent tables are never opened directly: routing is verified
#    semantically (what a fresh follower picks up from the checkpoint vs
#    what the demoted node still serves from ingest) and through the
#    verbose traces emitted by the step-down code paths.
@disagg_test_class
class test_layered_stepdown_async01(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered_stepdown_async01'
    nbase = 10

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'verbose=[layered:1],disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered_stepdown_async01', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def key(self, i):
        return 'key-%03d' % i

    # Assert that each pattern appears in the stdout produced since the last clean,
    # then consume the output. The step-down code paths trace their decisions under
    # verbose=[layered] and this is how the test watches them.
    def assertTraces(self, patterns):
        out = self.readStdout(30000)
        for pat in patterns:
            self.assertTrue(re.search(pat, out),
                'expected trace %r not found in: %r' % (pat, out))
        self.cleanStdout()

    def assertTrace(self, pattern):
        self.assertTraces([pattern])

    # Assert that a pattern does NOT appear in the stdout produced since the last
    # clean, then consume the output.
    def assertNoTrace(self, pattern):
        out = self.readStdout(30000)
        self.assertFalse(re.search(pattern, out),
            'unexpected trace %r found in: %r' % (pattern, out))
        self.cleanStdout()

    def create_with_base_content(self):
        self.ignoreStdoutPattern('WT_VERB_LAYERED')
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, self.nbase + 1):
            cursor[self.key(i)] = 'base'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        return cursor

    def count_keys(self, cursor):
        cursor.reset()
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.reset()
        return count

    def open_follower_conn(self):
        return self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')

    # The main end-to-end scenario: routing on both sides of the cutoff, double-write
    # resolution, the step-down checkpoint, what a fresh follower sees, and what the
    # demoted ex-leader still serves from its ingest table.
    def test_routing_and_visibility(self):
        cursor = self.create_with_base_content()

        # Arm the cutoff.
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Commit timestamp known up front and at/below the cutoff: single write to stable.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(90))
        cursor['key-A'] = 'pre-cutoff'
        self.assertTrace(r'stepdown: routed write to stable \(commit_ts=90, cutoff=100\)')
        self.session.commit_transaction()

        # Commit timestamp known up front and after the cutoff: single write to ingest.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-B'] = 'post-cutoff'
        self.assertTrace(r'stepdown: routed write to ingest \(commit_ts=110, cutoff=100\)')
        self.session.commit_transaction()

        # Commit timestamp only known at commit: double-write, resolved to the stable
        # side because the transaction commits at/below the cutoff.
        self.session.begin_transaction()
        cursor['key-C'] = 'twin-pre'
        self.assertTraces([
            r'stepdown: commit timestamp unknown, double-writing both constituents \(cutoff=100\)',
            r'stepdown: marked stable copy of a double-write',
            r'stepdown: marked ingest copy of a double-write'])
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(95))
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=95, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=95, cutoff=100\)'])

        # Double-write update of the same key, still on the stable side.
        self.session.begin_transaction()
        cursor['key-C'] = 'twin-pre2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(96))
        self.assertTrace(r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=96, cutoff=100\)')

        # Double-write resolved to the ingest side: the transaction commits after the cutoff.
        self.session.begin_transaction()
        cursor['key-D'] = 'twin-post'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(120))
        self.assertTrace(r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=120, cutoff=100\)')

        # Double-write remove of a base key, resolved to the ingest side: the stable
        # copy of the tombstone is aborted, so the checkpoint below keeps the key.
        self.session.begin_transaction()
        cursor.set_key(self.key(1))
        self.assertEqual(cursor.remove(), 0)
        self.assertTrace(r'stepdown: commit timestamp unknown, double-writing')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(125))
        self.assertTrace(r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=125, cutoff=100\)')

        # Double-write modify, resolved to the ingest side.
        self.session.begin_transaction()
        cursor.set_key('key-C')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('X', 0, 1)]), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(130))
        self.assertTrace(r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=130, cutoff=100\)')
        self.cleanStdout()

        # Reads on the leader during the window merge both constituents.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(cursor['key-A'], 'pre-cutoff')
        self.assertEqual(cursor['key-B'], 'post-cutoff')
        self.assertEqual(cursor['key-C'], 'Xwin-pre2')
        self.assertEqual(cursor['key-D'], 'twin-post')
        cursor.set_key(self.key(1))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 4)
        self.session.rollback_transaction()

        # The step-down checkpoint, taken exactly at the cutoff.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        # A fresh follower picking up that checkpoint sees exactly the pre-cutoff
        # content: key-A and key-C@96, but no key-B/key-D, and key-001 still present
        # because its removal committed after the cutoff.
        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(cursor_follow['key-A'], 'pre-cutoff')
        self.assertEqual(cursor_follow['key-C'], 'twin-pre2')
        cursor_follow.set_key('key-B')
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        cursor_follow.set_key('key-D')
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        cursor_follow.set_key(self.key(1))
        self.assertEqual(cursor_follow.search(), 0)
        self.assertEqual(self.count_keys(cursor_follow), self.nbase + 2)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # Elegant demote of the old leader: no restart, the cutoff is cleared, open
        # cursors keep working, and the post-cutoff content is still served from the
        # local ingest table - exactly where a follower would have it.
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(cursor['key-B'], 'post-cutoff')
        self.assertEqual(cursor['key-D'], 'twin-post')
        self.assertEqual(cursor['key-C'], 'Xwin-pre2')
        cursor.set_key(self.key(1))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 4)
        self.session.rollback_transaction()
        cursor.close()
        self.cleanStdout()

    # The guard rails: straddler rejection, mandatory commit timestamps, the prepared
    # transaction restriction, arming validation and the checkpoint guard.
    def test_guards(self):
        cursor = self.create_with_base_content()

        # Two straddler transactions write before the cutoff is armed, without a
        # commit timestamp. Their content sits in the stable table and cannot be
        # rerouted, so committing after the cutoff must fail; at/below it is fine.
        session2 = self.conn.open_session('')
        cursor2 = session2.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor['key-S1'] = 'straddler'
        session2.begin_transaction()
        cursor2['key-S2'] = 'ok'

        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(120)),
            '/must not commit after the prepare-to-step-down timestamp/')
        self.assertTrace(r'stepdown: rejecting straddler commit \(commit_ts=120, cutoff=100\)')

        session2.commit_transaction('commit_timestamp=' + self.timestamp_str(95))
        cursor2.close()
        session2.close()
        self.cleanStdout()

        # During the window, a transaction touching a layered table must carry a
        # commit timestamp: it decides which side of the cutoff the content is on.
        self.session.begin_transaction()
        cursor['key-U'] = 'no-timestamp'
        self.assertTrace(r'stepdown: commit timestamp unknown, double-writing')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(),
            '/commit timestamp is required for transactions writing to layered tables/')
        self.assertTrace(r'stepdown: rejecting commit with no commit timestamp')

        # Prepared transactions cannot contain double-writes (FIXME-WT-17785).
        self.session.begin_transaction()
        cursor['key-P'] = 'prepared'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.prepare_transaction(
                'prepare_timestamp=' + self.timestamp_str(105)),
            '/cannot include step-down double-writes/')
        self.session.rollback_transaction()
        self.cleanStdout()

        # Arming requires the leader role.
        conn_follow = self.open_follower_conn()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: conn_follow.set_timestamp(
                'prepare_to_step_down=' + self.timestamp_str(50)),
            '/requires the disaggregated leader role/')
        conn_follow.close()

        # The cutoff must not move while armed; abandoning (zero) and re-arming works.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.set_timestamp(
                'prepare_to_step_down=' + self.timestamp_str(150)),
            '/a step-down is already prepared at timestamp/')
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(200))
        self.assertTrace(r'stepdown: cutoff armed at 200')

        # A checkpoint must not pass the cutoff: it would silently exclude the
        # content redirected to ingest.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.checkpoint(),
            '/must not be later than the prepare-to-step-down timestamp/')

        # Clear the cutoff so the shutdown checkpoint at stable=250 is legal.
        self.conn.set_timestamp('prepare_to_step_down=0')
        cursor.close()
        self.cleanStdout()

    # A rejected straddler must leave the world clean: the failed commit rolls the
    # transaction back completely, the session stays usable, the write can be
    # retried, and a straddler committing exactly at the cutoff is allowed and
    # included in the step-down checkpoint.
    def test_straddler_rollback(self):
        cursor = self.create_with_base_content()

        # Two transactions write before the cutoff exists, with no commit timestamp.
        session2 = self.conn.open_session('')
        cursor2 = session2.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor['key-S1'] = 'must-roll-back'
        session2.begin_transaction()
        cursor2['key-S2'] = 'boundary'

        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Committing the straddler after the cutoff fails...
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(120)),
            '/must not commit after the prepare-to-step-down timestamp/')
        self.assertTrace(r'stepdown: rejecting straddler commit \(commit_ts=120, cutoff=100\)')

        # ...and rolls the transaction back: the session is immediately reusable and
        # nothing the transaction wrote is visible in either constituent.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key('key-S1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # Committing exactly at the cutoff is allowed: the cutoff itself belongs to
        # the stable side and therefore to the step-down checkpoint.
        session2.commit_transaction('commit_timestamp=' + self.timestamp_str(100))
        self.assertNoTrace(r'stepdown: rejecting')
        cursor2.close()
        session2.close()

        # The rejected write can be retried at a usable timestamp.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(90))
        cursor['key-S1'] = 'retried'
        self.assertTrace(r'stepdown: routed write to stable \(commit_ts=90, cutoff=100\)')
        self.session.commit_transaction()

        # Both survivors are part of the step-down checkpoint.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow['key-S1'], 'retried')
        self.assertEqual(cursor_follow['key-S2'], 'boundary')
        self.assertEqual(self.count_keys(cursor_follow), self.nbase + 2)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # Explicit and implicit rollbacks during the window: double-writes and routed
    # writes vanish from both constituents, resolution never runs, and the keys can
    # be rewritten afterward.
    def test_rollback_scenarios(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Explicit rollback of a double-write: both copies die, no resolution runs.
        self.session.begin_transaction()
        cursor['key-R1'] = 'rolled-back'
        self.assertTrace(r'stepdown: commit timestamp unknown, double-writing')
        self.session.rollback_transaction()
        self.assertNoTrace(r'stepdown: resolved double-write')

        # Explicit rollback of a write routed to ingest.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-R2'] = 'rolled-back'
        self.assertTrace(r'stepdown: routed write to ingest \(commit_ts=110, cutoff=100\)')
        self.session.rollback_transaction()

        # A commit rejected for having no timestamp also rolls the transaction back.
        self.session.begin_transaction()
        cursor['key-R3'] = 'rolled-back'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.commit_transaction(),
            '/commit timestamp is required/')
        self.cleanStdout()

        # None of it is visible, from either constituent.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        for k in ['key-R1', 'key-R2', 'key-R3']:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # The same key can be rewritten and committed normally afterward.
        self.session.begin_transaction()
        cursor['key-R1'] = 'second-attempt'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(95))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=95, cutoff=100\)')

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['key-R1'], 'second-attempt')
        self.session.rollback_transaction()

        self.conn.set_timestamp('prepare_to_step_down=0')
        cursor.close()
        self.cleanStdout()

    # Transactions that mix timestamp knowledge: the vectored-write shape (several
    # commit timestamps in one transaction), double-writes followed by routed writes
    # once the timestamp is set, the same key double-written twice, and reserve.
    def test_mixed_timestamp_transactions(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # One transaction, two commit timestamps on opposite sides of the cutoff:
        # each update is routed by the timestamp in effect when it was made.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(95))
        cursor['key-M'] = 'pre-cutoff'
        self.assertTrace(r'stepdown: routed write to stable \(commit_ts=95, cutoff=100\)')
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-N'] = 'post-cutoff'
        self.assertTrace(r'stepdown: routed write to ingest \(commit_ts=110, cutoff=100\)')
        self.session.commit_transaction()
        self.assertNoTrace(r'stepdown: resolved double-write')

        # Double-writes made before the commit timestamp was known resolve with it;
        # later writes in the same transaction route directly.
        self.session.begin_transaction()
        cursor['key-X'] = 'twin'
        self.assertTrace(r'stepdown: commit timestamp unknown, double-writing')
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(120))
        cursor['key-Y'] = 'routed'
        self.assertTrace(r'stepdown: routed write to ingest \(commit_ts=120, cutoff=100\)')
        self.session.commit_transaction()
        self.assertTrace(
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=120, cutoff=100\)')

        # The same key double-written twice in one transaction: both pairs resolve.
        self.session.begin_transaction()
        cursor['key-Z'] = 'first'
        cursor['key-Z'] = 'second'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(125))
        out = self.readStdout(30000)
        self.assertEqual(
            len(re.findall(r'aborting the stable copy \(commit_ts=125, cutoff=100\)', out)), 2)
        self.cleanStdout()

        # Reserve participates in the double-write; reserves are discarded at commit
        # as usual and the following update resolves normally.
        self.session.begin_transaction()
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.reserve(), 0)
        cursor[self.key(2)] = 'reserved-then-updated'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(96))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=96, cutoff=100\)')

        # Reads on the leader during the window see all of it.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['key-M'], 'pre-cutoff')
        self.assertEqual(cursor['key-N'], 'post-cutoff')
        self.assertEqual(cursor['key-X'], 'twin')
        self.assertEqual(cursor['key-Y'], 'routed')
        self.assertEqual(cursor['key-Z'], 'second')
        self.assertEqual(cursor[self.key(2)], 'reserved-then-updated')
        self.assertEqual(self.count_keys(cursor), self.nbase + 5)
        self.session.rollback_transaction()

        # The step-down checkpoint carries exactly the pre-cutoff side.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow['key-M'], 'pre-cutoff')
        self.assertEqual(cursor_follow[self.key(2)], 'reserved-then-updated')
        for k in ['key-N', 'key-X', 'key-Y', 'key-Z']:
            cursor_follow.set_key(k)
            self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor_follow), self.nbase + 1)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # The demoted ex-leader still serves the post-cutoff side from ingest.
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['key-N'], 'post-cutoff')
        self.assertEqual(cursor['key-X'], 'twin')
        self.assertEqual(cursor['key-Y'], 'routed')
        self.assertEqual(cursor['key-Z'], 'second')
        self.assertEqual(self.count_keys(cursor), self.nbase + 5)
        self.session.rollback_transaction()
        cursor.close()
        self.cleanStdout()

    # Iteration and search_near on a leader during the window merge the two
    # constituents in key order, with ingest versions shadowing stable ones.
    def test_iteration_and_search_near(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Post-cutoff content in ingest: an update shadowing a stable key, an insert
        # between two stable keys, and a tombstone hiding a stable key.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor[self.key(2)] = 'updated'
        cursor['key-0045'] = 'between'
        self.session.commit_transaction()
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(115))
        cursor.set_key(self.key(3))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction()
        self.cleanStdout()

        expected = ['key-001', 'key-002', 'key-004', 'key-0045', 'key-005',
                    'key-006', 'key-007', 'key-008', 'key-009', 'key-010']

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        got = []
        while cursor.next() == 0:
            got.append(cursor.get_key())
        self.assertEqual(got, expected)
        cursor.reset()
        got = []
        while cursor.prev() == 0:
            got.append(cursor.get_key())
        self.assertEqual(got, list(reversed(expected)))
        cursor.reset()
        self.assertEqual(cursor[self.key(2)], 'updated')
        self.session.rollback_transaction()

        # The pre-window history is intact underneath the ingest content.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(2)], 'base')
        self.assertEqual(cursor[self.key(3)], 'base')
        cursor.set_key('key-0045')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # search_near merges both constituents: probing next to the ingest-resident
        # key lands on it, probing the removed key lands on its stable neighbour.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key('key-0044')
        exact = cursor.search_near()
        self.assertTrue(exact > 0)
        self.assertEqual(cursor.get_key(), 'key-0045')
        cursor.reset()
        cursor.set_key(self.key(3))
        exact = cursor.search_near()
        self.assertTrue(exact > 0)
        self.assertEqual(cursor.get_key(), 'key-004')
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Abandoning a prepared step-down (fail-back). Until the fail-back drain exists
    # (FIXME-WT-17785), content redirected to ingest is stranded on an unarmed
    # leader: this test documents the current gap, not the desired behaviour.
    def test_failback_documents_gap(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-F'] = 'redirected'
        self.assertTrace(r'stepdown: routed write to ingest')
        self.session.commit_transaction()

        # Visible while the window is open...
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        self.assertEqual(cursor['key-F'], 'redirected')
        self.session.rollback_transaction()

        # ...stranded once the step-down is abandoned: an unarmed leader does not
        # read the ingest constituent and no checkpoint will include this content.
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        cursor.set_key('key-F')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # The content is not lost, only unreachable: re-arming makes it visible.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        self.assertEqual(cursor['key-F'], 'redirected')
        self.session.rollback_transaction()

        # After abandoning again, writes behave like a normal leader.
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.cleanStdout()
        self.session.begin_transaction()
        cursor['key-G'] = 'normal'
        self.assertNoTrace(r'stepdown: ')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(130))

        # Leave the window armed: with content stranded in ingest, an unarmed leader
        # fails layered verification ("ingest on leader must be empty") - the same
        # gap seen from the other side. Until the fail-back drain exists, the only
        # consistent end states are armed or demoted.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(200))

        cursor.close()
        self.cleanStdout()
