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
#      - every write to a layered table goes to both constituents
#        ("double-write"); when the commit timestamp is assigned, the copy on
#        the wrong side of the cutoff is aborted (ts <= T keeps the stable
#        copy, ts > T keeps the ingest copy), resolved per update so a
#        vectored transaction places each update by its own timestamp,
#      - routing a write whose commit timestamp is already known directly to a
#        single constituent is a deferred optimization (FIXME-WT-17785),
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

        # Commit timestamp known up front and at/below the cutoff: still a double-write,
        # the ingest copy is aborted when the timestamp is assigned.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(90))
        cursor['key-A'] = 'pre-cutoff'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction()
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=90, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=90, cutoff=100\)'])

        # Commit timestamp known up front and after the cutoff: double-write, the stable
        # copy is aborted, leaving the content in ingest.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-B'] = 'post-cutoff'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction()
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=110, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)'])

        # Commit timestamp only known at commit: double-write, resolved to the stable
        # side because the transaction commits at/below the cutoff.
        self.session.begin_transaction()
        cursor['key-C'] = 'twin-pre'
        self.assertTraces([
            r'stepdown: double-writing both constituents \(cutoff=100\)',
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
        self.assertTrace(r'stepdown: double-writing both constituents')
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
        self.assertTrace(r'stepdown: double-writing both constituents')
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
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction()
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=90, cutoff=100\)')

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
        self.assertTrace(r'stepdown: double-writing both constituents')
        self.session.rollback_transaction()
        self.assertNoTrace(r'stepdown: resolved double-write')

        # Explicit rollback of a double-write whose commit timestamp is already known.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-R2'] = 'rolled-back'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
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
    # commit timestamps in one transaction), the same key double-written twice, and
    # reserve. Every write double-writes; resolution is per update, so a vectored
    # transaction still places each update by its own commit timestamp.
    def test_mixed_timestamp_transactions(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # One transaction, two commit timestamps on opposite sides of the cutoff: each
        # update double-writes and is resolved by the timestamp in effect when it was
        # made, so the pre-cutoff update keeps its stable copy and the post-cutoff
        # update keeps its ingest copy.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(95))
        cursor['key-M'] = 'pre-cutoff'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['key-N'] = 'post-cutoff'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction()
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=95, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=95, cutoff=100\)',
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=110, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)'])

        # A double-write made before the commit timestamp is known, then a later write
        # once it is set: both resolve by the transaction's commit timestamp (120),
        # ending up on the ingest side.
        self.session.begin_transaction()
        cursor['key-X'] = 'twin'
        self.assertTrace(r'stepdown: double-writing both constituents')
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(120))
        cursor['key-Y'] = 'routed'
        self.assertTrace(r'stepdown: double-writing both constituents')
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
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
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

    # Reconfiguring the cutoff: a step-down can be abandoned and re-armed at a different
    # timestamp, and subsequent writes are routed by the new cutoff. A write at 150 -
    # which would have been post-cutoff under 100 - is pre-cutoff under 200.
    def test_rearm_at_new_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()

        # Arm then abandon without a step-down: writes behave like a normal leader.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')
        self.session.begin_transaction()
        cursor['key-normal'] = 'leader'
        self.assertNoTrace(r'stepdown: ')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(120))

        # Re-arm at a higher cutoff and write on both sides of it.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(200))
        self.assertTrace(r'stepdown: cutoff armed at 200')

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(150))
        cursor['key-P'] = 'below-new-cutoff'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=200\)')
        self.session.commit_transaction()
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=150, cutoff=200\)')

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(250))
        cursor['key-Q'] = 'above-new-cutoff'
        self.session.commit_transaction()
        self.assertTrace(
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=250, cutoff=200\)')
        self.cleanStdout()

        # The step-down checkpoint at the new cutoff carries key-P and key-normal, but
        # not the post-cutoff key-Q.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=200 \(cutoff=200\)')

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(300))
        self.assertEqual(cursor_follow['key-P'], 'below-new-cutoff')
        self.assertEqual(cursor_follow['key-normal'], 'leader')
        cursor_follow.set_key('key-Q')
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # The demoted node still serves the post-cutoff key-Q from ingest.
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(300))
        self.assertEqual(cursor['key-P'], 'below-new-cutoff')
        self.assertEqual(cursor['key-Q'], 'above-new-cutoff')
        self.session.rollback_transaction()
        cursor.close()
        self.cleanStdout()

    # The same key written on both sides of the cutoff in separate transactions: the
    # pre-cutoff version is kept in stable (and the step-down checkpoint) while the
    # post-cutoff version shadows it from ingest. The two constituents hold two
    # different versions of the one key, each surfacing where it should.
    def test_key_updated_across_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Update a base key pre-cutoff (kept in stable) ...
        self.session.begin_transaction()
        cursor[self.key(1)] = 'pre'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=90, cutoff=100\)')

        # ... then again post-cutoff (kept in ingest, shadowing the stable version).
        self.session.begin_transaction()
        cursor[self.key(1)] = 'post'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.assertTrace(
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)')
        self.cleanStdout()

        # On the leader, each read timestamp sees the right version of the one key.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(1)], 'base')
        self.session.rollback_transaction()
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(95))
        self.assertEqual(cursor[self.key(1)], 'pre')
        self.session.rollback_transaction()
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor[self.key(1)], 'post')
        self.session.rollback_transaction()

        # The step-down checkpoint carries the pre-cutoff version from stable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow[self.key(1)], 'pre')
        self.assertEqual(self.count_keys(cursor_follow), self.nbase)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # The demoted node serves the post-cutoff version from its ingest table.
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor[self.key(1)], 'post')
        self.session.rollback_transaction()
        cursor.close()
        self.cleanStdout()

    # Insert with overwrite=false during the window detects duplicates against both
    # constituents: a key just inserted into ingest, and a key that exists only in
    # stable. This exercises the existence check that runs because a leader's writes
    # may now target ingest.
    def test_insert_duplicate_detection(self):
        cursor = self.create_with_base_content()
        nodup = self.session.open_cursor(self.uri, None, 'overwrite=false')
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # A brand-new key inserted after the cutoff is routed to ingest and succeeds.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        nodup.set_key('key-new')
        nodup.set_value('first')
        self.assertEqual(nodup.insert(), 0)
        self.session.commit_transaction()

        # Re-inserting it fails: the duplicate is detected in the ingest constituent.
        self.session.begin_transaction()
        nodup.set_key('key-new')
        nodup.set_value('second')
        self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
            lambda: nodup.insert(), '/WT_DUPLICATE_KEY/')
        self.session.rollback_transaction()

        # Inserting a key that exists only in stable (a base key) also fails, so the
        # check consults both constituents while a step-down is armed.
        self.session.begin_transaction()
        nodup.set_key(self.key(1))
        nodup.set_value('dup')
        self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
            lambda: nodup.insert(), '/WT_DUPLICATE_KEY/')
        self.session.rollback_transaction()

        # The new key still has only its original value.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['key-new'], 'first')
        self.session.rollback_transaction()

        # key-new is committed to ingest; leave the cutoff armed so teardown's layered
        # verification (an unarmed leader must have an empty ingest) is satisfied.
        nodup.close()
        cursor.close()
        self.cleanStdout()

    # Read-your-own-writes through a double-write: the writing transaction sees its own
    # uncommitted writes (merged from the constituents), while a concurrent session does
    # not, and both become visible to others once committed.
    def test_read_your_own_writes(self):
        cursor = self.create_with_base_content()
        session2 = self.conn.open_session('')
        cursor2 = session2.open_cursor(self.uri)
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # The writer double-writes a new key and an update to a base key, with no commit
        # timestamp yet, and reads both back within the same transaction.
        self.session.begin_transaction()
        cursor['key-RYOW'] = 'mine'
        cursor[self.key(2)] = 'changed'
        self.assertEqual(cursor['key-RYOW'], 'mine')
        self.assertEqual(cursor[self.key(2)], 'changed')

        # A concurrent reader sees neither the new key nor the change.
        session2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor2.set_key('key-RYOW')
        self.assertEqual(cursor2.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor2[self.key(2)], 'base')
        session2.rollback_transaction()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(95))

        # After committing at/below the cutoff, both are visible to the other session.
        session2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor2['key-RYOW'], 'mine')
        self.assertEqual(cursor2[self.key(2)], 'changed')
        session2.rollback_transaction()

        cursor2.close()
        session2.close()
        self.conn.set_timestamp('prepare_to_step_down=0')
        cursor.close()
        self.cleanStdout()

    # A checkpoint at a stable timestamp below the cutoff is allowed (only one past the
    # cutoff is rejected). It captures the pre-cutoff stable content and excludes the
    # post-cutoff content redirected to ingest, just like the step-down checkpoint at
    # the cutoff - the cutoff, not the exact stable timestamp, decides the split.
    def test_checkpoint_below_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # A pre-cutoff write (stable) and a post-cutoff write (ingest).
        self.session.begin_transaction()
        cursor['key-pre'] = 'stable-side'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.session.begin_transaction()
        cursor['key-post'] = 'ingest-side'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.cleanStdout()

        # A checkpoint at stable=95 (below the cutoff) is accepted.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(95))
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=95 \(cutoff=100\)')

        # The follower picks up the pre-cutoff content but not the ingest-resident one.
        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow['key-pre'], 'stable-side')
        cursor_follow.set_key('key-post')
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # key-post is committed to ingest; leave the cutoff armed for teardown.
        cursor.close()
        self.cleanStdout()

    # The cutoff itself belongs to the stable side: a double-write committed exactly at
    # the cutoff keeps its stable copy, one tick past it keeps the ingest copy.
    def test_commit_at_exact_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor['key-at'] = 'at-cutoff'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(100))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=100, cutoff=100\)')

        self.session.begin_transaction()
        cursor['key-past'] = 'past-cutoff'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(101))
        self.assertTrace(
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=101, cutoff=100\)')
        self.cleanStdout()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow['key-at'], 'at-cutoff')
        cursor_follow.set_key('key-past')
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # A large transaction of double-writes resolved in one commit past the cutoff: every
    # update lands in ingest, all are visible on the leader, and none reach the step-down
    # checkpoint.
    def test_large_batch_one_timestamp(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        n = 30
        self.session.begin_transaction()
        for i in range(n):
            cursor['batch-%03d' % i] = 'v%d' % i
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(150))
        self.cleanStdout()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(160))
        for i in range(n):
            self.assertEqual(cursor['batch-%03d' % i], 'v%d' % i)
        self.assertEqual(self.count_keys(cursor), self.nbase + n)
        self.session.rollback_transaction()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(160))
        self.assertEqual(self.count_keys(cursor_follow), self.nbase)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # A remove committed at/below the cutoff keeps its stable tombstone, so the key is
    # absent from the step-down checkpoint a follower picks up.
    def test_remove_before_cutoff_in_checkpoint(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=90, cutoff=100\)')
        self.cleanStdout()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()

        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor_follow.set_key(self.key(2))
        self.assertEqual(cursor_follow.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor_follow), self.nbase - 1)
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # The cutoff is connection-wide: writes to every layered table are routed by the same
    # prepare-to-step-down timestamp.
    def test_multiple_layered_tables(self):
        uri_b = 'layered:test_layered_stepdown_async01_b'
        cursor = self.create_with_base_content()
        self.session.create(uri_b, 'key_format=S,value_format=S')
        cursor_b = self.session.open_cursor(uri_b)
        self.session.begin_transaction()
        cursor_b['b-base'] = 'base'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor['a-pre'] = 'a-pre'
        cursor_b['b-pre'] = 'b-pre'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.session.begin_transaction()
        cursor['a-post'] = 'a-post'
        cursor_b['b-post'] = 'b-post'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.cleanStdout()

        # Both tables show all content while armed.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['a-pre'], 'a-pre')
        self.assertEqual(cursor['a-post'], 'a-post')
        self.assertEqual(cursor_b['b-pre'], 'b-pre')
        self.assertEqual(cursor_b['b-post'], 'b-post')
        self.session.rollback_transaction()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')

        # After demote both tables still serve their post-cutoff content from ingest.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['a-post'], 'a-post')
        self.assertEqual(cursor_b['b-post'], 'b-post')
        self.assertEqual(cursor['a-pre'], 'a-pre')
        self.assertEqual(cursor_b['b-pre'], 'b-pre')
        self.session.rollback_transaction()
        cursor_b.close()
        cursor.close()
        self.cleanStdout()

    # A read-only transaction during the window needs no commit timestamp: it writes no
    # layered content, so neither the double-write nor the straddler guard applies.
    def test_readonly_txn_no_timestamp(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(1)], 'base')
        self.session.commit_transaction()

        self.session.begin_transaction()
        cursor.set_key(self.key(99))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.commit_transaction()
        self.assertNoTrace(r'stepdown: rejecting')

        cursor.close()
        self.cleanStdout()

    # Repeated writes and a remove of one key within a single transaction during the
    # window: read-your-own-writes tracks the churn and the final write wins.
    def test_intra_txn_key_churn(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor['churn'] = 'v1'
        cursor['churn'] = 'v2'
        cursor.set_key('churn')
        self.assertEqual(cursor.remove(), 0)
        cursor['churn'] = 'v3'
        self.assertEqual(cursor['churn'], 'v3')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.cleanStdout()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['churn'], 'v3')
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Modify of a base key after the cutoff: the modified value lands in ingest while the
    # stable copy (the base value) is what the step-down checkpoint keeps.
    def test_modify_across_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor.set_key(self.key(1))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('Z', 0, 1)]), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.assertTrace(
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)')
        self.cleanStdout()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor[self.key(1)], 'Zase')
        self.session.rollback_transaction()
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(1)], 'base')
        self.session.rollback_transaction()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.session.checkpoint()
        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor_follow[self.key(1)], 'base')
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # A key that exists only in ingest (inserted after the cutoff) can be removed in a
    # later transaction: the stable-side remove finds nothing, so the double-write falls
    # back to a lone ingest tombstone, which is the surviving side (it commits after the
    # cutoff). A remove of a key absent from both constituents still returns WT_NOTFOUND.
    def test_ingest_only_key_remove(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Insert a key after the cutoff: it lives only in ingest.
        self.session.begin_transaction()
        cursor['io-key'] = 'ingest-only'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))

        # Removing a genuinely missing key still fails.
        self.session.begin_transaction()
        cursor.set_key('io-missing')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # Removing the ingest-only key succeeds.
        self.session.begin_transaction()
        cursor.set_key('io-key')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(120))
        self.cleanStdout()

        # It is gone on the leader.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key('io-key')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # And gone on the demoted node, which serves it from ingest.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key('io-key')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A key that exists only in ingest (inserted after the cutoff) can be modified: the
    # ingest side builds the new value from the ingest base, and the stable side - which
    # has no base - is skipped, leaving the lone ingest copy as the surviving side.
    def test_modify_ingest_only_key(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor['im-key'] = 'first'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))

        self.session.begin_transaction()
        cursor.set_key('im-key')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('X', 0, 1)]), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(120))
        self.cleanStdout()

        # The modified value is visible on the leader.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['im-key'], 'Xirst')
        self.session.rollback_transaction()

        # And on the demoted node, which serves it from ingest.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(self.conn)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['im-key'], 'Xirst')
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A long single-transaction CRUD batch committed pre-cutoff: insert five new keys,
    # update three base keys, remove two base keys, with read-your-own-writes checks
    # interleaved, then one commit timestamp at/below the cutoff for the whole batch.
    def test_crud_long_single_ts_precutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        for i in range(1, 6):
            cursor['new-%03d' % i] = 'ins-%d' % i
        cursor[self.key(1)] = 'upd-1'
        cursor[self.key(2)] = 'upd-2'
        cursor[self.key(3)] = 'upd-3'
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.remove(), 0)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.remove(), 0)

        # Read-your-own-writes: inserts and updates are visible, removed keys are gone,
        # untouched base keys still read 'base', regardless of the (unset) commit ts.
        for i in range(1, 6):
            self.assertEqual(cursor['new-%03d' % i], 'ins-%d' % i)
        self.assertEqual(cursor[self.key(1)], 'upd-1')
        self.assertEqual(cursor[self.key(2)], 'upd-2')
        self.assertEqual(cursor[self.key(3)], 'upd-3')
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(6)], 'base')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.cleanStdout()

        # After commit at 90, a read above it sees the whole batch as the final state.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(95))
        for i in range(1, 6):
            self.assertEqual(cursor['new-%03d' % i], 'ins-%d' % i)
        self.assertEqual(cursor[self.key(1)], 'upd-1')
        self.assertEqual(cursor[self.key(2)], 'upd-2')
        self.assertEqual(cursor[self.key(3)], 'upd-3')
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        for i in range(6, self.nbase + 1):
            self.assertEqual(cursor[self.key(i)], 'base')
        self.assertEqual(self.count_keys(cursor), self.nbase + 5 - 2)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # The same long single-transaction CRUD batch committed post-cutoff: one commit
    # timestamp above the cutoff places the whole batch on the ingest side, but the
    # leader-visible final state is identical.
    def test_crud_long_single_ts_postcutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        for i in range(1, 6):
            cursor['new-%03d' % i] = 'ins-%d' % i
        cursor[self.key(1)] = 'upd-1'
        cursor[self.key(2)] = 'upd-2'
        cursor[self.key(3)] = 'upd-3'
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.remove(), 0)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.remove(), 0)

        # Read-your-own-writes sees the same final state inside the writing transaction.
        for i in range(1, 6):
            self.assertEqual(cursor['new-%03d' % i], 'ins-%d' % i)
        self.assertEqual(cursor[self.key(1)], 'upd-1')
        self.assertEqual(cursor[self.key(2)], 'upd-2')
        self.assertEqual(cursor[self.key(3)], 'upd-3')
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(7)], 'base')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(130))
        self.cleanStdout()

        # After commit at 130, a read above it sees the whole batch as the final state.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(140))
        for i in range(1, 6):
            self.assertEqual(cursor['new-%03d' % i], 'ins-%d' % i)
        self.assertEqual(cursor[self.key(1)], 'upd-1')
        self.assertEqual(cursor[self.key(2)], 'upd-2')
        self.assertEqual(cursor[self.key(3)], 'upd-3')
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        for i in range(6, self.nbase + 1):
            self.assertEqual(cursor[self.key(i)], 'base')
        self.assertEqual(self.count_keys(cursor), self.nbase + 5 - 2)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Commit timestamp set up front and pre-cutoff (90): one long CRUD sequence of
    # inserts, updates and a remove, all committing on the stable side, with
    # read-your-own-writes during the txn and explicit reads afterwards.
    def test_crud_ts_before_ops_pre_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Timestamp known before any op: every update commits at 90 (stable side).
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(90))
        # Insert several new keys.
        cursor['ins-1'] = 'new1'
        cursor['ins-2'] = 'new2'
        cursor['ins-3'] = 'new3'
        # Update several: two base keys (last committed at 20, so 90 is monotonic) and
        # one of the just-inserted keys.
        cursor[self.key(2)] = 'upd2'
        cursor[self.key(3)] = 'upd3'
        cursor['ins-1'] = 'new1b'
        # Remove one base key.
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.remove(), 0)
        # Read-your-own-writes: the latest written value regardless of timestamp, and
        # the removed key is not present.
        self.assertEqual(cursor['ins-1'], 'new1b')
        self.assertEqual(cursor['ins-2'], 'new2')
        self.assertEqual(cursor['ins-3'], 'new3')
        self.assertEqual(cursor[self.key(2)], 'upd2')
        self.assertEqual(cursor[self.key(3)], 'upd3')
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # Commit with no further timestamp: uses the 90 set before the ops.
        self.session.commit_transaction()

        # Read after commit at a timestamp well past 90: every committed version is
        # visible, the inserted keys carry their final values, the updated base keys
        # carry their new values, and the removed base key is hidden by its tombstone.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(200))
        self.assertEqual(cursor['ins-1'], 'new1b')
        self.assertEqual(cursor['ins-2'], 'new2')
        self.assertEqual(cursor['ins-3'], 'new3')
        self.assertEqual(cursor[self.key(2)], 'upd2')
        self.assertEqual(cursor[self.key(3)], 'upd3')
        cursor.set_key(self.key(5))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # Untouched base key still reads 'base'.
        self.assertEqual(cursor[self.key(1)], 'base')
        # Net key count: 10 base - 1 removed + 3 inserted = 12.
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 3)
        self.session.rollback_transaction()

        # A read before the commit timestamp sees none of this transaction's writes:
        # the inserts are absent and the base keys keep their ts-20 values.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        cursor.set_key('ins-1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(2)], 'base')
        self.assertEqual(cursor[self.key(5)], 'base')
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Commit timestamp set up front and post-cutoff (130): one long CRUD sequence of
    # inserts, updates and a remove, all committing on the ingest side, with
    # read-your-own-writes during the txn and explicit reads afterwards.
    def test_crud_ts_before_ops_post_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Timestamp known before any op: every update commits at 130 (ingest side).
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(130))
        # Insert several new keys.
        cursor['post-1'] = 'p1'
        cursor['post-2'] = 'p2'
        cursor['post-3'] = 'p3'
        # Update several: a base key (last committed at 20, so 130 is monotonic) and
        # two of the just-inserted keys.
        cursor[self.key(4)] = 'upd4'
        cursor['post-1'] = 'p1b'
        cursor['post-2'] = 'p2b'
        # Remove one base key.
        cursor.set_key(self.key(7))
        self.assertEqual(cursor.remove(), 0)
        # Read-your-own-writes: the latest written value regardless of timestamp, and
        # the removed key is not present.
        self.assertEqual(cursor['post-1'], 'p1b')
        self.assertEqual(cursor['post-2'], 'p2b')
        self.assertEqual(cursor['post-3'], 'p3')
        self.assertEqual(cursor[self.key(4)], 'upd4')
        cursor.set_key(self.key(7))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # Commit with no further timestamp: uses the 130 set before the ops.
        self.session.commit_transaction()

        # Read after commit at a timestamp past 130: every committed version is
        # visible, the inserted keys carry their final values, the updated base key
        # carries its new value, and the removed base key is hidden by its tombstone.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(200))
        self.assertEqual(cursor['post-1'], 'p1b')
        self.assertEqual(cursor['post-2'], 'p2b')
        self.assertEqual(cursor['post-3'], 'p3')
        self.assertEqual(cursor[self.key(4)], 'upd4')
        cursor.set_key(self.key(7))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # Untouched base key still reads 'base'.
        self.assertEqual(cursor[self.key(1)], 'base')
        # Net key count: 10 base - 1 removed + 3 inserted = 12.
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 3)
        self.session.rollback_transaction()

        # A read between the cutoff and the commit timestamp (110, after 100 but before
        # 130) still predates these writes: the inserts are absent and the touched
        # base keys keep their ts-20 values.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(110))
        cursor.set_key('post-1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(4)], 'base')
        self.assertEqual(cursor[self.key(7)], 'base')
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # post-* and the key-007 tombstone committed to ingest; leave the cutoff armed
        # for teardown.
        cursor.close()
        self.cleanStdout()

    # One vectored transaction straddling the cutoff: insert+update at 95 (stable),
    # insert+remove at 130 (ingest), then a final update at 160. Each op commits at
    # the timestamp in effect when it ran, so reads at 96/135/170 see distinct states.
    def test_crud_vectored_straddle_insert_update_remove(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        # First batch commits at 95 (stable side).
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(95))
        cursor['key-V1'] = 'early'
        cursor[self.key(1)] = 'upd95'
        self.assertEqual(cursor['key-V1'], 'early')
        self.assertEqual(cursor[self.key(1)], 'upd95')

        # Second batch commits at 130 (ingest side): a new key and a base-key removal.
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(130))
        cursor['key-V2'] = 'late'
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor['key-V2'], 'late')
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

        # Third batch commits at 160: re-update key-V1 (monotonic, last touched at 95).
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(160))
        cursor['key-V1'] = 'final160'
        self.assertEqual(cursor['key-V1'], 'final160')

        self.session.commit_transaction()

        # At read 96: only the 95 batch is committed-visible. key-V1 is its 95 value,
        # key-001 is the 95 update, key-V2 (130) is absent, key-002's removal (130) is
        # not yet effective so it is still 'base'.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(96))
        self.assertEqual(cursor['key-V1'], 'early')
        self.assertEqual(cursor[self.key(1)], 'upd95')
        cursor.set_key('key-V2')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(2)], 'base')
        self.session.rollback_transaction()

        # At read 135: key-V1's newest version <=135 is still the 95 'early' (160 not
        # yet); key-V2 (130) is now visible; key-002's tombstone (130) hides it.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(cursor['key-V1'], 'early')
        self.assertEqual(cursor['key-V2'], 'late')
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # At read 170: key-V1's newest version is now the 160 'final160'; key-V2 stays
        # visible and key-002 stays removed.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(170))
        self.assertEqual(cursor['key-V1'], 'final160')
        self.assertEqual(cursor['key-V2'], 'late')
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A vectored transaction that seeds a base key on the stable side at 90, then
    # modifies it and inserts another key on the ingest side at 140. modify operates
    # on the read-your-own-writes value, so the visible result depends on the read ts.
    def test_crud_vectored_modify_search_across_cutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        # First batch commits at 90 (stable side): set a known value on a base key and
        # seed a fresh key.
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(90))
        cursor[self.key(3)] = 'mod-base'
        cursor['key-W'] = 'seed90'
        self.assertEqual(cursor[self.key(3)], 'mod-base')
        self.assertEqual(cursor['key-W'], 'seed90')

        # Second batch commits at 140 (ingest side): modify the base key (it currently
        # holds 'mod-base') and insert another key.
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(140))
        cursor.set_key(self.key(3))
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('X', 0, 1)]), 0)
        cursor['key-W2'] = 'ingest140'
        self.assertEqual(cursor[self.key(3)], 'Xod-base')
        self.assertEqual(cursor['key-W2'], 'ingest140')

        self.session.commit_transaction()

        # At read 95: only the 90 batch is visible. key-003 is 'mod-base' (the modify
        # at 140 is in the future), key-W is 'seed90', key-W2 (140) is absent.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(95))
        self.assertEqual(cursor[self.key(3)], 'mod-base')
        self.assertEqual(cursor['key-W'], 'seed90')
        cursor.set_key('key-W2')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # At read 145: the 140 modify is now visible, turning 'mod-base' into
        # 'Xod-base'; key-W2 (140) is also present; key-W remains its 90 value.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(145))
        self.assertEqual(cursor[self.key(3)], 'Xod-base')
        self.assertEqual(cursor['key-W2'], 'ingest140')
        self.assertEqual(cursor['key-W'], 'seed90')
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Full lifecycle of one key across five transactions during the window:
    # insert@90, update@110, modify@120, remove@140, re-insert@160, with the
    # monotonic-per-key rule respected. Historical reads between the steps pin
    # each version: every read at R sees the newest committed version with
    # commit ts <= R (a tombstone hides the key).
    def test_crud_single_key_full_lifecycle_historical_reads(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        L = 'key-L'

        # txn1: insert at ts 90 (pre-cutoff, stable side). Read-your-own-writes
        # sees the new value regardless of timestamp.
        self.session.begin_transaction()
        cursor[L] = 'born'
        self.assertEqual(cursor[L], 'born')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))

        # txn2: update at ts 110 (post-cutoff, ingest side). Monotonic: 110 >= 90.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor[L] = 'grown'
        self.assertEqual(cursor[L], 'grown')
        self.session.commit_transaction()

        # txn3: modify at ts 120 (>= 110). The visible base of the modify is the
        # newest committed value, 'grown'; Modify('G',0,1) replaces the first byte
        # to make 'Grown'.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(120))
        cursor.set_key(L)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('G', 0, 1)]), 0)
        self.assertEqual(cursor[L], 'Grown')
        self.session.commit_transaction()

        # txn4: remove at ts 140 (>= 120). Read-your-own-writes sees it absent.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(140))
        cursor.set_key(L)
        self.assertEqual(cursor.remove(), 0)
        cursor.set_key(L)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.commit_transaction()

        # txn5: re-insert at ts 160 (>= 140). Read-your-own-writes sees the new value.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(160))
        cursor[L] = 'reborn'
        self.assertEqual(cursor[L], 'reborn')
        self.session.commit_transaction()
        self.cleanStdout()

        # Historical reads pin each version by its commit timestamp.

        # Below the first insert: the key does not exist yet.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(85))
        cursor.set_key(L)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # At 100: newest version with ts <= 100 is the insert@90.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(100))
        self.assertEqual(cursor[L], 'born')
        self.session.rollback_transaction()

        # At 115: newest with ts <= 115 is the update@110.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(115))
        self.assertEqual(cursor[L], 'grown')
        self.session.rollback_transaction()

        # At 130: newest with ts <= 130 is the modify@120.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor[L], 'Grown')
        self.session.rollback_transaction()

        # At 150: newest with ts <= 150 is the tombstone@140; the key is hidden.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(150))
        cursor.set_key(L)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # At 170: newest with ts <= 170 is the re-insert@160.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(170))
        self.assertEqual(cursor[L], 'reborn')
        self.assertEqual(self.count_keys(cursor), self.nbase + 1)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A permutation matrix in one pre-cutoff transaction: six keys each take a
    # different CRUD shape (insert; update; remove; insert+update; insert+remove;
    # update+remove), committed at a single timestamp on the stable side.
    def test_crud_permutation_matrix_precutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # keyA inserted; keyB (base) updated; keyC (base) removed; keyD inserted then
        # updated; keyE inserted then removed; keyF (base) updated then removed.
        self.session.begin_transaction()
        cursor['pm-A'] = 'A1'
        cursor[self.key(2)] = 'B-upd'
        cursor.set_key(self.key(3))
        self.assertEqual(cursor.remove(), 0)
        cursor['pm-D'] = 'D1'
        cursor['pm-D'] = 'D2'
        cursor['pm-E'] = 'E1'
        cursor.set_key('pm-E')
        self.assertEqual(cursor.remove(), 0)
        cursor[self.key(4)] = 'F-upd'
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.remove(), 0)

        # Read-your-own-writes before commit: net outcome of each shape is visible.
        self.assertEqual(cursor['pm-A'], 'A1')
        self.assertEqual(cursor[self.key(2)], 'B-upd')
        cursor.set_key(self.key(3))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor['pm-D'], 'D2')
        cursor.set_key('pm-E')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(4))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # 10 base, minus key-003 and key-004 removed, plus pm-A and pm-D inserted.
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))

        # After commit, a read at 95 sees each key's net outcome.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(95))
        self.assertEqual(cursor['pm-A'], 'A1')
        self.assertEqual(cursor[self.key(2)], 'B-upd')
        self.assertEqual(cursor['pm-D'], 'D2')
        for absent in [self.key(3), 'pm-E', self.key(4)]:
            cursor.set_key(absent)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # Before this transaction (at the base commit ts 20) all base keys are intact
        # and none of the new keys exist yet.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(2)], 'base')
        self.assertEqual(cursor[self.key(3)], 'base')
        self.assertEqual(cursor[self.key(4)], 'base')
        cursor.set_key('pm-A')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # The same permutation matrix in one post-cutoff transaction: six keys each take
    # a different CRUD shape, committed at a single timestamp on the ingest side.
    def test_crud_permutation_matrix_postcutoff(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # keyA inserted; keyB (base) updated; keyC (base) removed; keyD inserted then
        # updated; keyE inserted then removed; keyF (base) updated then removed.
        self.session.begin_transaction()
        cursor['pm-A'] = 'A1'
        cursor[self.key(5)] = 'B-upd'
        cursor.set_key(self.key(6))
        self.assertEqual(cursor.remove(), 0)
        cursor['pm-D'] = 'D1'
        cursor['pm-D'] = 'D2'
        cursor['pm-E'] = 'E1'
        cursor.set_key('pm-E')
        self.assertEqual(cursor.remove(), 0)
        cursor[self.key(7)] = 'F-upd'
        cursor.set_key(self.key(7))
        self.assertEqual(cursor.remove(), 0)

        # Read-your-own-writes before commit: net outcome of each shape is visible.
        self.assertEqual(cursor['pm-A'], 'A1')
        self.assertEqual(cursor[self.key(5)], 'B-upd')
        cursor.set_key(self.key(6))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor['pm-D'], 'D2')
        cursor.set_key('pm-E')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(7))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        # 10 base, minus key-006 and key-007 removed, plus pm-A and pm-D inserted.
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(130))

        # After commit, a read at 140 sees each key's net outcome.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(140))
        self.assertEqual(cursor['pm-A'], 'A1')
        self.assertEqual(cursor[self.key(5)], 'B-upd')
        self.assertEqual(cursor['pm-D'], 'D2')
        for absent in [self.key(6), 'pm-E', self.key(7)]:
            cursor.set_key(absent)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # Reading before this transaction's commit ts (but after the base ts) sees the
        # untouched base world: the removed/updated base keys are still 'base'.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(110))
        self.assertEqual(cursor[self.key(5)], 'base')
        self.assertEqual(cursor[self.key(6)], 'base')
        self.assertEqual(cursor[self.key(7)], 'base')
        cursor.set_key('pm-A')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A sequence of five transactions with different timestamp patterns builds
    # cumulative state on a shared key set (S1..S4 plus base keys) during the
    # window: ts-after, ts-before, a vectored two-timestamp transaction, then
    # two more ts-after transactions. Each key's history is monotonic, and the
    # cumulative and historical state is verified by reads at several timestamps.
    def test_crud_multi_txn_sequence_cumulative_state(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # txn1, timestamp set only at commit (90): insert S1, S2 and update a base key.
        self.session.begin_transaction()
        cursor['S1'] = 's1a'
        cursor['S2'] = 's2a'
        cursor[self.key(1)] = 'k1-90'
        self.assertEqual(cursor['S1'], 's1a')
        self.assertEqual(cursor[self.key(1)], 'k1-90')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))

        # txn2, timestamp set before the ops (110): update S1, insert S3.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(110))
        cursor['S1'] = 's1b'
        cursor['S3'] = 's3a'
        self.session.commit_transaction()

        # txn3, vectored: modify S2 at 120, then remove a base key and insert S4 at 140.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(120))
        cursor.set_key('S2')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('Z', 0, 1)]), 0)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(140))
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.remove(), 0)
        cursor['S4'] = 's4a'
        self.assertEqual(cursor['S2'], 'Z2a')
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.commit_transaction()

        # txn4, timestamp set only at commit (160): remove S3.
        self.session.begin_transaction()
        cursor.set_key('S3')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(160))

        # txn5, timestamp set only at commit (180): re-insert S3 with a new value.
        self.session.begin_transaction()
        cursor['S3'] = 's3b'
        self.assertEqual(cursor['S3'], 's3b')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(180))
        self.cleanStdout()

        # Cumulative state at a high read timestamp: newest committed version of each key.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(200))
        self.assertEqual(cursor['S1'], 's1b')
        self.assertEqual(cursor['S2'], 'Z2a')
        self.assertEqual(cursor['S3'], 's3b')
        self.assertEqual(cursor['S4'], 's4a')
        self.assertEqual(cursor[self.key(1)], 'k1-90')
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 4)
        self.session.rollback_transaction()

        # At read 100: only txn1 is committed (90); S3/S4 not yet inserted, S2 unmodified,
        # the base key removal (140) not yet visible, so all base keys remain.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(100))
        self.assertEqual(cursor['S1'], 's1a')
        self.assertEqual(cursor['S2'], 's2a')
        self.assertEqual(cursor[self.key(1)], 'k1-90')
        self.assertEqual(cursor[self.key(2)], 'base')
        cursor.set_key('S3')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key('S4')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase + 2)
        self.session.rollback_transaction()

        # At read 130: txn2 (S1=s1b, S3=s3a) and the modify of S2 (120) are visible, but
        # the base-key removal and S4 (both 140) are not, and S3 is still its first value.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(cursor['S1'], 's1b')
        self.assertEqual(cursor['S2'], 'Z2a')
        self.assertEqual(cursor['S3'], 's3a')
        self.assertEqual(cursor[self.key(2)], 'base')
        cursor.set_key('S4')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase + 3)
        self.session.rollback_transaction()

        # At read 170: the base-key removal (140), S4 (140) and the S3 tombstone (160) are
        # visible, but the S3 re-insert (180) is not, so S3 is absent here.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(170))
        self.assertEqual(cursor['S4'], 's4a')
        cursor.set_key('S3')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase - 1 + 3)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # A long post-cutoff transaction that interleaves writes with reads: it reads its
    # own writes (regardless of the still-unassigned commit timestamp) and reads
    # untouched base keys (still 'base') between writes. A read at a pre-window
    # timestamp then confirms the old history is intact - base keys 'base', the new
    # keys absent - and a read at a high timestamp confirms the new state.
    def test_crud_interleaved_reads_with_prewindow_history(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # One write transaction, commit timestamp assigned only at commit (130, after
        # the cutoff). Reads between the writes mix read-your-own-writes with reads of
        # untouched base keys.
        self.session.begin_transaction()
        cursor['il-a'] = 'a1'
        self.assertEqual(cursor['il-a'], 'a1')
        # An untouched base key still reads 'base' inside this transaction.
        self.assertEqual(cursor[self.key(1)], 'base')
        cursor['il-b'] = 'b1'
        # Update a base key, then read it back and another untouched base key.
        cursor[self.key(5)] = 'five-new'
        self.assertEqual(cursor[self.key(5)], 'five-new')
        self.assertEqual(cursor[self.key(10)], 'base')
        # Update the first new key, and confirm read-your-own-writes sees the latest.
        cursor['il-a'] = 'a2'
        self.assertEqual(cursor['il-a'], 'a2')
        cursor['il-c'] = 'c1'
        self.assertEqual(cursor['il-b'], 'b1')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(130))
        self.cleanStdout()

        # A read in the pre-window past sees the original history only: the base keys
        # are 'base' (committed at 20), key-005 is still 'base' (its update committed at
        # 130 is invisible at 50), and the new keys do not exist yet.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(50))
        self.assertEqual(cursor[self.key(1)], 'base')
        self.assertEqual(cursor[self.key(5)], 'base')
        self.assertEqual(cursor[self.key(10)], 'base')
        for k in ['il-a', 'il-b', 'il-c']:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(self.count_keys(cursor), self.nbase)
        self.session.rollback_transaction()

        # A read after the commit timestamp sees the new state: the new keys with their
        # latest values and the updated base key.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(140))
        self.assertEqual(cursor['il-a'], 'a2')
        self.assertEqual(cursor['il-b'], 'b1')
        self.assertEqual(cursor['il-c'], 'c1')
        self.assertEqual(cursor[self.key(5)], 'five-new')
        self.assertEqual(cursor[self.key(1)], 'base')
        self.assertEqual(self.count_keys(cursor), self.nbase + 3)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()

    # Duplicate-key detection through an overwrite=false cursor interleaved with a long
    # CRUD transaction during the window: a fresh insert succeeds, re-inserting it fails
    # as a duplicate, inserting an existing base key fails as a duplicate, and ordinary
    # CRUD around those checks proceeds normally. A failed insert raises but does not
    # roll the transaction back, so the surrounding work still commits.
    def test_crud_duplicate_detection_in_long_crud_txn(self):
        cursor = self.create_with_base_content()
        nodup = self.session.open_cursor(self.uri, None, 'overwrite=false')
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # One transaction, commit timestamp assigned at commit (140, after the cutoff).
        self.session.begin_transaction()

        # A brand-new key inserted through the no-overwrite cursor succeeds.
        nodup.set_key('dc-1')
        nodup.set_value('one')
        self.assertEqual(nodup.insert(), 0)
        self.assertEqual(cursor['dc-1'], 'one')

        # Re-inserting the just-inserted key fails as a duplicate (it now exists in
        # this transaction's view).
        nodup.set_key('dc-1')
        nodup.set_value('dup')
        self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
            lambda: nodup.insert(), '/WT_DUPLICATE_KEY/')

        # Ordinary CRUD continues in the same transaction: insert another new key and
        # update a base key through the overwrite cursor.
        cursor['dc-2'] = 'two'
        cursor[self.key(3)] = 'three-new'
        self.assertEqual(cursor['dc-2'], 'two')

        # Inserting an existing base key (lives only in stable) also fails as a
        # duplicate, so the existence check consults both constituents.
        nodup.set_key(self.key(1))
        nodup.set_value('dup')
        self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
            lambda: nodup.insert(), '/WT_DUPLICATE_KEY/')

        # The failed inserts did not disturb the transaction: read-your-own-writes still
        # holds and a remove of one new key takes effect.
        cursor.set_key('dc-1')
        self.assertEqual(cursor.remove(), 0)
        cursor.set_key('dc-1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor[self.key(3)], 'three-new')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(140))
        self.cleanStdout()

        # After commit: dc-1 was removed, dc-2 and the base-key update survive.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(150))
        cursor.set_key('dc-1')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor['dc-2'], 'two')
        self.assertEqual(cursor[self.key(3)], 'three-new')
        self.assertEqual(self.count_keys(cursor), self.nbase + 1)
        self.session.rollback_transaction()

        nodup.close()
        cursor.close()
        self.cleanStdout()

    # Reconfigure: a read cursor opened BEFORE the step-down keeps serving correct values
    # across the window and the demote (the "reads continue across step-down" goal). The
    # traces confirm each write took the double-write/resolve path and the role flip.
    def test_reconfig_reader_survives_demote(self):
        cursor = self.create_with_base_content()
        s2 = self.conn.open_session('')
        reader = s2.open_cursor(self.uri)
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Pre-cutoff write: kept on the stable side.
        self.session.begin_transaction()
        cursor[self.key(1)] = 'pre'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=90, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=90, cutoff=100\)'])

        # Post-cutoff write: kept on the ingest side.
        self.session.begin_transaction()
        cursor['key-post'] = 'post'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=110, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)'])
        self.cleanStdout()

        # The pre-opened reader (separate session) sees both while armed.
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(reader[self.key(1)], 'pre')
        self.assertEqual(reader['key-post'], 'post')
        s2.rollback_transaction()

        # Step-down checkpoint, then demote.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)

        # The SAME reader cursor, opened before step-down, still serves both values after
        # the demote (post-cutoff content from the local ingest table).
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(reader[self.key(1)], 'pre')
        self.assertEqual(reader['key-post'], 'post')
        s2.rollback_transaction()

        reader.close()
        s2.close()
        cursor.close()
        self.cleanStdout()

    # Reconfigure context with a CONCURRENT reader: while the writer holds an uncommitted
    # double-write, another session sees nothing; after commit it sees the resolved value.
    # Traces confirm the twin marking and which side the commit timestamp kept.
    def test_reconfig_concurrent_reader_isolation(self):
        cursor = self.create_with_base_content()
        s2 = self.conn.open_session('')
        reader = s2.open_cursor(self.uri)
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # Writer double-writes a new key, commit timestamp not yet known.
        self.session.begin_transaction()
        cursor['cc-key'] = 'new'
        self.assertTraces([
            r'stepdown: double-writing both constituents \(cutoff=100\)',
            r'stepdown: marked stable copy of a double-write',
            r'stepdown: marked ingest copy of a double-write'])

        # A concurrent reader does not see the uncommitted write.
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        reader.set_key('cc-key')
        self.assertEqual(reader.search(), wiredtiger.WT_NOTFOUND)
        s2.rollback_transaction()

        # Commit post-cutoff: ingest copy kept, stable copy aborted.
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=110, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=110, cutoff=100\)'])
        self.cleanStdout()

        # Now the reader sees the committed value.
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        self.assertEqual(reader['cc-key'], 'new')
        s2.rollback_transaction()

        reader.close()
        s2.close()
        cursor.close()
        self.cleanStdout()

    # Reconfigure cycle (abandon then re-arm) observed through a reader, with traces for
    # each transition. Content redirected to ingest is reachable while armed, hidden once
    # abandoned (unarmed leaders skip ingest), and reachable again after re-arming.
    def test_reconfig_abandon_rearm_reader(self):
        cursor = self.create_with_base_content()
        s2 = self.conn.open_session('')
        reader = s2.open_cursor(self.uri)
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        cursor['ar-key'] = 'redirected'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=110, cutoff=100\)')
        self.cleanStdout()

        # Reachable while armed.
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        self.assertEqual(reader['ar-key'], 'redirected')
        s2.rollback_transaction()

        # Abandon: an unarmed leader stops reading ingest, so the reader no longer sees it.
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        reader.set_key('ar-key')
        self.assertEqual(reader.search(), wiredtiger.WT_NOTFOUND)
        s2.rollback_transaction()

        # Re-arm: reachable again.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(120))
        self.assertEqual(reader['ar-key'], 'redirected')
        s2.rollback_transaction()

        reader.close()
        s2.close()
        cursor.close()
        self.cleanStdout()

    # A vectored CRUD transaction with full path traces and readers at multiple read
    # timestamps: one update kept on the stable side (commit ts 95), one on the ingest
    # side (commit ts 130), each placed by its own timestamp.
    def test_reconfig_vectored_crud_traces_readers(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(95))
        cursor['key-V1'] = 'v1'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(130))
        cursor['key-V2'] = 'v2'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=100\)')
        # Read-your-own-writes inside the transaction.
        self.assertEqual(cursor['key-V1'], 'v1')
        self.assertEqual(cursor['key-V2'], 'v2')
        self.session.commit_transaction()
        self.assertTraces([
            r'stepdown: resolved double-write, keeping the stable copy \(commit_ts=95, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the ingest copy \(commit_ts=95, cutoff=100\)',
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=130, cutoff=100\)',
            r'stepdown: resolved double-write, aborting the stable copy \(commit_ts=130, cutoff=100\)'])
        self.cleanStdout()

        # Readers confirm per-update placement timing on the leader.
        s2 = self.conn.open_session('')
        r = s2.open_cursor(self.uri)
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(96))
        self.assertEqual(r['key-V1'], 'v1')
        r.set_key('key-V2')
        self.assertEqual(r.search(), wiredtiger.WT_NOTFOUND)
        s2.rollback_transaction()
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(r['key-V1'], 'v1')
        self.assertEqual(r['key-V2'], 'v2')
        s2.rollback_transaction()
        r.close()
        s2.close()

        # The step-down checkpoint keeps only the pre-cutoff update; a fresh follower agrees.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(100))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=100 \(cutoff=100\)')
        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        sf = conn_follow.open_session('')
        cf = sf.open_cursor(self.uri)
        sf.begin_transaction('read_timestamp=' + self.timestamp_str(135))
        self.assertEqual(cf['key-V1'], 'v1')
        cf.set_key('key-V2')
        self.assertEqual(cf.search(), wiredtiger.WT_NOTFOUND)
        sf.rollback_transaction()
        cf.close()
        sf.close()
        conn_follow.close()

        cursor.close()
        self.cleanStdout()

    # Multiple reconfigure cycles: arm then abandon with no content (normal-leader writes
    # in between emit no step-down trace), then re-arm at a new cutoff where a write is
    # routed and resolved to ingest. A reader sees both the normal and the ingest content.
    def test_reconfig_multiple_cycles(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()

        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')
        self.conn.set_timestamp('prepare_to_step_down=0')
        self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')

        # A normal leader write between cycles takes no step-down path.
        self.session.begin_transaction()
        cursor['c-norm'] = 'leader'
        self.assertNoTrace(r'stepdown: ')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))

        # Re-arm at a higher cutoff and route a write to ingest.
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(200))
        self.assertTrace(r'stepdown: cutoff armed at 200')
        self.session.begin_transaction()
        cursor['c-ing'] = 'post'
        self.assertTrace(r'stepdown: double-writing both constituents \(cutoff=200\)')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(250))
        self.assertTrace(
            r'stepdown: resolved double-write, keeping the ingest copy \(commit_ts=250, cutoff=200\)')
        self.cleanStdout()

        # A reader sees the normal (stable) content and the routed (ingest) content.
        s2 = self.conn.open_session('')
        r = s2.open_cursor(self.uri)
        s2.begin_transaction('read_timestamp=' + self.timestamp_str(300))
        self.assertEqual(r['c-norm'], 'leader')
        self.assertEqual(r['c-ing'], 'post')
        s2.rollback_transaction()
        r.close()
        s2.close()

        cursor.close()
        self.cleanStdout()

    # Regression: under twin-only, a key that is logically absent but still STALE-resident in
    # the stable constituent (removed post-cutoff, so that remove kept the ingest tombstone and
    # aborted the stable tombstone). A second remove must return WT_NOTFOUND (not 0, and not a
    # consecutive ingest tombstone), and a modify of an absent key must return WT_NOTFOUND (not
    # crash by building the modify on an empty base).
    def test_twin_absent_key_remove_modify(self):
        cursor = self.create_with_base_content()
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(100))
        self.assertTrace(r'stepdown: cutoff armed at 100')

        # X: put pre-cutoff (stable), then remove post-cutoff (ingest tombstone over stale stable).
        self.session.begin_transaction()
        cursor['X'] = 'v1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.session.begin_transaction()
        cursor.set_key('X')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(110))

        # X is logically absent: merged read and a second remove agree.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(130))
        cursor.set_key('X')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        self.session.begin_transaction()
        cursor.set_key('X')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # Z: same setup, then a modify of the absent key must fail cleanly (not crash).
        self.session.begin_transaction()
        cursor['Z'] = 'zv1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(90))
        self.session.begin_transaction()
        cursor.set_key('Z')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(112))
        self.session.begin_transaction()
        cursor.set_key('Z')
        self.assertEqual(cursor.modify([wiredtiger.Modify('!', 0, 0)]), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        # A key that never existed: remove and modify both return WT_NOTFOUND.
        self.session.begin_transaction()
        cursor.set_key('never')
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        cursor.set_key('never')
        self.assertEqual(cursor.modify([wiredtiger.Modify('!', 0, 0)]), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()

        cursor.close()
        self.cleanStdout()
