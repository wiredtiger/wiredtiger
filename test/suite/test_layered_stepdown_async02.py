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

import random, re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_stepdown_async02.py
#    Model-based differential STRESS test for the twin-only elegant step-down
#    feature (FIXME-WT-17785).
#
#    While a prepare-to-step-down cutoff is armed on the leader, every layered
#    write double-writes to both constituents and each update is resolved at
#    commit by its own commit timestamp (<= cutoff keeps the stable copy, >
#    cutoff keeps the ingest copy). The leader merges both constituents under
#    standard MVCC.
#
#    The strategy here is to drive a long stream of randomized, LEGAL
#    transactions against the engine while applying the identical operations to
#    a pure-Python reference model of the leader-visible committed history. We
#    then assert that the engine's MVCC reads (at several historical read
#    timestamps), its read-your-own-writes, and its merged forward/reverse
#    iteration all agree with the model. Because the timestamp scheme uses a
#    single global strictly-increasing counter, per-key monotonicity is
#    guaranteed and a key can only ever migrate stable -> ingest (never the
#    reverse), so the workload stays inside the documented legality envelope.
#
#    Constituent placement is verified semantically, exactly as async01 does:
#    a fresh follower picking up the step-down checkpoint must see the
#    stable-side projection (everything committed at ts <= cutoff), and the
#    demoted ex-leader must keep serving the post-cutoff content from ingest.
TOMBSTONE = object()  # Sentinel for a removed key in the reference model.

@disagg_test_class
class test_layered_stepdown_async02(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered_stepdown_async02'
    nbase = 10

    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'verbose=[layered:1],disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered_stepdown_async02', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def key(self, i):
        return 'key-%03d' % i

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

    # ----- reference model ------------------------------------------------
    #
    # self.model maps a key to a list of (commit_ts, value_or_TOMBSTONE) entries
    # in strictly increasing commit_ts order. model_visible() answers "what is
    # the newest committed version at or before read timestamp R" - exactly the
    # MVCC contract the layered cursor must honour by merging the two
    # constituents.

    def model_record(self, key, commit_ts, value):
        self.model.setdefault(key, []).append((commit_ts, value))

    def model_visible(self, key, read_ts):
        best = None
        for (ts, value) in self.model.get(key, []):
            if ts <= read_ts:
                best = value
            else:
                break
        if best is None or best is TOMBSTONE:
            return None
        return best

    def model_present_keys(self, read_ts):
        return set(k for k in self.model if self.model_visible(k, read_ts) is not None)

    def model_ordered_keys(self, read_ts):
        return sorted(self.model_present_keys(read_ts))

    # Project the model onto the stable side of the cutoff: this is what a
    # follower (or the step-down checkpoint) would hold - only versions
    # committed at ts <= cutoff exist.
    def model_visible_stable(self, key, read_ts, cutoff):
        best = None
        for (ts, value) in self.model.get(key, []):
            if ts > cutoff:
                break
            if ts <= read_ts:
                best = value
            else:
                break
        if best is None or best is TOMBSTONE:
            return None
        return best

    # ----- assertion helpers ---------------------------------------------

    # Assert the engine's visible value for every sampled key, at a given read
    # timestamp, matches the model. Uses a passed-in session/cursor so a
    # concurrent reader on a second session can run the same check.
    def assert_reads_match(self, session, cursor, read_ts, keys):
        session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        try:
            for k in keys:
                expect = self.model_visible(k, read_ts)
                cursor.set_key(k)
                ret = cursor.search()
                if expect is None:
                    self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
                        self.fail_ctx('search(%r)@%d expected NOTFOUND, model=%r' %
                            (k, read_ts, expect)))
                else:
                    self.assertEqual(ret, 0,
                        self.fail_ctx('search(%r)@%d expected %r, got NOTFOUND' %
                            (k, read_ts, expect)))
                    self.assertEqual(cursor.get_value(), expect,
                        self.fail_ctx('value(%r)@%d engine=%r model=%r' %
                            (k, read_ts, cursor.get_value(), expect)))
        finally:
            session.rollback_transaction()

    # Assert forward and reverse full-table iteration match the model's ordered
    # key list, and that the value at each key is right.
    def assert_iteration_matches(self, cursor, read_ts):
        expected = self.model_ordered_keys(read_ts)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        try:
            fwd = []
            cursor.reset()
            while cursor.next() == 0:
                fwd.append(cursor.get_key())
            self.assertEqual(fwd, expected,
                self.fail_ctx('forward iter@%d engine=%r model=%r' % (read_ts, fwd, expected)))
            rev = []
            cursor.reset()
            while cursor.prev() == 0:
                rev.append(cursor.get_key())
            self.assertEqual(rev, list(reversed(expected)),
                self.fail_ctx('reverse iter@%d engine=%r model=%r' %
                    (read_ts, rev, list(reversed(expected)))))
            # Spot-check the values surfaced by iteration as well.
            cursor.reset()
            while cursor.next() == 0:
                k = cursor.get_key()
                self.assertEqual(cursor.get_value(), self.model_visible(k, read_ts),
                    self.fail_ctx('iter value(%r)@%d engine=%r model=%r' %
                        (k, read_ts, cursor.get_value(), self.model_visible(k, read_ts))))
            cursor.reset()
        finally:
            self.session.rollback_transaction()

    # Render the deterministic seed plus the operation log so any failure is
    # immediately reproducible and debuggable.
    def fail_ctx(self, msg):
        log = '\n'.join(self.oplog[-40:])
        return ('\n%s\nseed=%d\nlast ops:\n%s' % (msg, self.seed, log))

    # ----- the randomized differential driver -----------------------------

    # Drive `ntxns` randomized transactions against a cutoff armed at `cutoff`.
    # The global timestamp counter starts below the cutoff so early
    # transactions land on the stable side and later ones cross onto the ingest
    # side; once it passes the cutoff it never goes back, so no key is ever
    # touched at a timestamp below its latest version (rule 2) and no key
    # migrates ingest -> stable (rule 4).
    def run_random_workload(self, cursor, cutoff, ntxns):
        keyspace = [self.key(i) for i in range(1, self.nbase + 1)]
        keyspace += ['hot-%03d' % i for i in range(0, 12)]
        new_idx = [0]

        # Span the global counter from a little below the cutoff to well above
        # it, so roughly the first third of the work is pre-cutoff.
        ts = cutoff - max(8, ntxns // 6)
        if ts <= 25:                 # stay clear of base content at ts 20 / stable
            ts = 26

        def fresh_key():
            new_idx[0] += 1
            k = 'gen-%04d' % new_idx[0]
            keyspace.append(k)
            return k

        for _ in range(ntxns):
            nops = random.randint(1, 8)
            # Pattern: 0 = set ts up front, 1 = set ts only at commit,
            # 2 = vectored (ts, ops, higher ts, more ops).
            pattern = random.randint(0, 2)

            self.session.begin_transaction()
            # txn_writes records (key, value_or_TOMBSTONE, op_ts) to apply to the
            # model once the transaction commits. Each write carries its own
            # commit timestamp so the vectored pattern is modelled exactly: the
            # engine resolves and timestamps each update by its own upd_start_ts.
            txn_writes = []
            # ryow tracks this txn's own latest write per key for read-your-own-writes.
            ryow = {}
            committed = True

            ts += 1
            op_ts = ts
            if pattern == 0:
                self.session.timestamp_transaction(
                    'commit_timestamp=' + self.timestamp_str(op_ts))

            for opno in range(nops):
                # In the vectored pattern, bump the commit timestamp partway
                # through with a strictly larger value (rule 2 within a txn).
                if pattern == 2 and opno == nops // 2:
                    ts += 1
                    op_ts = ts
                    self.session.timestamp_transaction(
                        'commit_timestamp=' + self.timestamp_str(op_ts))

                choice = random.random()
                k = random.choice(keyspace) if random.random() < 0.7 else fresh_key()

                # Current value as the engine should see it inside this txn:
                # this txn's own pending write if any, else the committed model.
                if k in ryow:
                    cur = ryow[k]
                else:
                    cur = self.model_visible(k, op_ts)

                if choice < 0.45:
                    # overwrite put (insert-or-update).
                    v = 'v%d-%d' % (op_ts, opno)
                    cursor[k] = v
                    self.oplog.append('PUT %s=%s @ts=%d' % (k, v, op_ts))
                    ryow[k] = v
                    txn_writes.append((k, v, op_ts))
                elif choice < 0.62:
                    # remove: legal only if the key currently exists for us.
                    cursor.set_key(k)
                    if cur is None:
                        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND,
                            self.fail_ctx('remove(%r) expected NOTFOUND (absent); op_ts=%d '
                                'in_ryow=%r model_hist=%r' %
                                (k, op_ts, k in ryow, self.model.get(k))))
                        self.oplog.append('REMOVE %s -> NOTFOUND (absent)' % k)
                    else:
                        self.assertEqual(cursor.remove(), 0,
                            self.fail_ctx('remove(%r) expected 0, present=%r' % (k, cur)))
                        self.oplog.append('REMOVE %s @ts=%d' % (k, op_ts))
                        ryow[k] = TOMBSTONE
                        txn_writes.append((k, TOMBSTONE, op_ts))
                elif choice < 0.80:
                    # modify: legal only if the key currently exists. Append a
                    # marker at the end so the transform is well-defined.
                    cursor.set_key(k)
                    if cur is None:
                        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                            self.fail_ctx('modify-precheck search(%r) expected NOTFOUND' % k))
                        self.oplog.append('MODIFY %s -> skip (absent)' % k)
                    else:
                        self.assertEqual(cursor.search(), 0,
                            self.fail_ctx('modify-precheck search(%r) expected present=%r' %
                                (k, cur)))
                        suffix = '#%d' % (op_ts % 100)
                        off = len(cur)
                        cursor.set_key(k)
                        self.assertEqual(
                            cursor.modify([wiredtiger.Modify(suffix, off, 0)]), 0,
                            self.fail_ctx('modify(%r) failed' % k))
                        newv = cur + suffix
                        self.oplog.append('MODIFY %s=%s @ts=%d' % (k, newv, op_ts))
                        ryow[k] = newv
                        txn_writes.append((k, newv, op_ts))
                else:
                    # read-your-own-writes / read check inside the open txn.
                    cursor.set_key(k)
                    ret = cursor.search()
                    if cur is None:
                        self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
                            self.fail_ctx('RYOW search(%r) expected NOTFOUND, model=%r' %
                                (k, cur)))
                    else:
                        self.assertEqual(ret, 0,
                            self.fail_ctx('RYOW search(%r) expected %r, got NOTFOUND' % (k, cur)))
                        self.assertEqual(cursor.get_value(), cur,
                            self.fail_ctx('RYOW value(%r) engine=%r model=%r' %
                                (k, cursor.get_value(), cur)))
                    self.oplog.append('READ %s -> %r' % (k, cur))

            # Occasionally roll the whole transaction back: nothing reaches the
            # model and both constituent copies must vanish.
            if random.random() < 0.08 and pattern != 0:
                self.session.rollback_transaction()
                committed = False
                self.oplog.append('ROLLBACK txn')
            else:
                if pattern == 1:
                    self.session.commit_transaction(
                        'commit_timestamp=' + self.timestamp_str(op_ts))
                else:
                    self.session.commit_transaction()

            if committed:
                self.apply_txn_to_model(txn_writes)

        return ts

    # Apply a committed transaction's writes to the model, each at its own commit
    # timestamp. Writes are recorded in op order (timestamps are non-decreasing
    # within a txn), so the per-key history stays sorted. Several writes to the
    # same key at the same timestamp collapse to the last one - matching the
    # engine, where the final update on the chain at that timestamp wins.
    def apply_txn_to_model(self, txn_writes):
        for (k, v, op_ts) in txn_writes:
            hist = self.model.setdefault(k, [])
            if hist and hist[-1][0] == op_ts:
                hist[-1] = (op_ts, v)
            else:
                hist.append((op_ts, v))

    # ----- tests ----------------------------------------------------------

    # The creative core: a long stream of randomized legal transactions on a
    # leader with an armed cutoff, checked against a reference model via MVCC
    # reads at several historical timestamps, read-your-own-writes, and merged
    # forward/reverse iteration. Ends with a demote that verifies stable-side
    # placement on a fresh follower and post-cutoff survival in ingest.
    def test_random_differential(self):
        self.seed = 42
        random.seed(self.seed)
        self.model = {}
        self.oplog = []
        for i in range(1, self.nbase + 1):
            self.model_record(self.key(i), 20, 'base')

        cursor = self.create_with_base_content()
        cutoff = 100000
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(cutoff))
        self.assertTrace(r'stepdown: cutoff armed at %d' % cutoff)

        ntxns = 300
        last_ts = self.run_random_workload(cursor, cutoff, ntxns)

        # Verify against the model at the current high read timestamp: every key
        # plus a full ordered-iteration check.
        all_keys = sorted(self.model.keys())
        self.assert_reads_match(self.session, cursor, last_ts + 5, all_keys)
        self.assert_iteration_matches(cursor, last_ts + 5)
        self.assertEqual(self.count_keys(cursor), len(self.model_present_keys(last_ts + 5)),
            self.fail_ctx('count mismatch at high ts'))

        # Verify at several historical read timestamps spread across the run,
        # including ones straddling the cutoff, on a CONCURRENT reader session.
        session2 = self.conn.open_session('')
        cursor2 = session2.open_cursor(self.uri)
        sample = random.sample(all_keys, min(25, len(all_keys)))
        for rts in [50, cutoff - 1, cutoff, cutoff + 1,
                    (cutoff + last_ts) // 2, last_ts]:
            self.assert_reads_match(session2, cursor2, rts, sample)
        cursor2.close()
        session2.close()

        # Snapshot the model's stable-side projection for the placement checks
        # below, before we demote.
        present_high = self.model_present_keys(last_ts + 5)

        # The step-down checkpoint at the cutoff captures the stable side.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(cutoff))
        self.cleanStdout()
        self.session.checkpoint()
        self.assertTrace(r'stepdown: step-down checkpoint at stable=%d \(cutoff=%d\)' %
            (cutoff, cutoff))

        # A fresh follower picking up that checkpoint must see exactly the
        # stable-side projection: only versions committed at ts <= cutoff.
        conn_follow = self.open_follower_conn()
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        read_ts = last_ts + 5
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        follow_expected = sorted(
            k for k in self.model if self.model_visible_stable(k, read_ts, cutoff) is not None)
        follow_got = []
        while cursor_follow.next() == 0:
            follow_got.append(cursor_follow.get_key())
        self.assertEqual(follow_got, follow_expected,
            self.fail_ctx('follower keys engine=%r model_stable=%r' %
                (follow_got, follow_expected)))
        cursor_follow.reset()
        for k in sample:
            expect = self.model_visible_stable(k, read_ts, cutoff)
            cursor_follow.set_key(k)
            ret = cursor_follow.search()
            if expect is None:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND,
                    self.fail_ctx('follower search(%r) expected NOTFOUND, stable-model=%r' %
                        (k, expect)))
            else:
                self.assertEqual(ret, 0,
                    self.fail_ctx('follower search(%r) expected %r, got NOTFOUND' % (k, expect)))
                self.assertEqual(cursor_follow.get_value(), expect,
                    self.fail_ctx('follower value(%r) engine=%r stable-model=%r' %
                        (k, cursor_follow.get_value(), expect)))
        session_follow.rollback_transaction()
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()

        # Demote the leader: it must keep serving the FULL merged content
        # (post-cutoff included) from its local ingest table.
        self.cleanStdout()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertTrace(r'stepdown: role flipped to follower, cutoff cleared')
        self.disagg_advance_checkpoint(self.conn)
        self.assert_reads_match(self.session, cursor, read_ts, sample)
        self.assert_iteration_matches(cursor, read_ts)
        self.assertEqual(self.count_keys(cursor), len(present_high),
            self.fail_ctx('demoted count mismatch'))
        cursor.close()
        self.cleanStdout()

    # Creative-but-legal stressors, each a self-contained phase on its own armed
    # cutoff, all checked against the model.
    def test_targeted_stressors(self):
        self.seed = 1234
        random.seed(self.seed)
        self.model = {}
        self.oplog = []
        for i in range(1, self.nbase + 1):
            self.model_record(self.key(i), 20, 'base')

        cursor = self.create_with_base_content()
        cutoff = 5000
        self.cleanStdout()
        self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(cutoff))
        self.assertTrace(r'stepdown: cutoff armed at %d' % cutoff)
        ts = 30

        # --- Long update chain on one key across the cutoff, pre and post. ---
        chain_key = 'chain'
        for i in range(40):
            ts += 1
            v = 'chain-v%03d' % i
            self.session.begin_transaction()
            cursor[chain_key] = v
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
            self.model_record(chain_key, ts, v)
            # Walk the counter across the cutoff midway through the chain.
            if i == 20:
                ts = max(ts, cutoff - 5)
            if i == 30:
                ts = max(ts, cutoff + 5)
        self.oplog.append('chain done, ts=%d' % ts)

        # --- Large values (a few KB), some pre- some post-cutoff. ---
        for i in range(6):
            ts += 1
            if i == 3:
                ts = max(ts, cutoff + 50)
            big = ('B%04d-' % i) + ('x' * 4096)
            self.session.begin_transaction()
            cursor['big-%02d' % i] = big
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
            self.model_record('big-%02d' % i, ts, big)

        # --- Same-key churn within one transaction: insert/remove/reinsert. ---
        ts += 1
        churn_ts = ts
        self.session.begin_transaction()
        cursor['churn'] = 'a'
        cursor.set_key('churn')
        self.assertEqual(cursor.remove(), 0)
        cursor['churn'] = 'b'
        cursor.set_key('churn')
        self.assertEqual(cursor.remove(), 0)
        cursor['churn'] = 'final'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(churn_ts))
        self.model_record('churn', churn_ts, 'final')

        # --- reserve() then update in a twin txn. ---
        ts += 1
        self.session.begin_transaction()
        cursor.set_key(self.key(2))
        self.assertEqual(cursor.reserve(), 0)
        cursor[self.key(2)] = 'reserved-updated'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        self.model_record(self.key(2), ts, 'reserved-updated')

        # --- Ingest-only key: insert post-cutoff, then remove AND modify it
        #     post-cutoff in later transactions (the just-fixed ingest-only
        #     path). The key never existed on the stable side. ---
        ts = max(ts, cutoff + 100)
        ts += 1
        io_remove_ts = ts
        self.session.begin_transaction()
        cursor['ingest-only-rm'] = 'io1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        self.model_record('ingest-only-rm', ts, 'io1')
        ts += 1
        self.session.begin_transaction()
        cursor.set_key('ingest-only-rm')
        self.assertEqual(cursor.remove(), 0)          # ingest-only remove
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        self.model_record('ingest-only-rm', ts, TOMBSTONE)

        ts += 1
        self.session.begin_transaction()
        cursor['ingest-only-mod'] = 'base-io'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        self.model_record('ingest-only-mod', ts, 'base-io')
        ts += 1
        self.session.begin_transaction()
        cursor.set_key('ingest-only-mod')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.modify([wiredtiger.Modify('-MOD', len('base-io'), 0)]), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        self.model_record('ingest-only-mod', ts, 'base-io-MOD')

        high = ts + 5

        # Verify everything at the high read ts: values, ordering, count.
        all_keys = sorted(self.model.keys())
        self.assert_reads_match(self.session, cursor, high, all_keys)
        self.assert_iteration_matches(cursor, high)
        self.assertEqual(self.count_keys(cursor), len(self.model_present_keys(high)),
            self.fail_ctx('count mismatch (targeted)'))

        # Historical reads through the chain show the right version at each step.
        for rts in [35, 45, cutoff - 1, cutoff + 1, cutoff + 60, high]:
            self.assert_reads_match(self.session, cursor, rts, all_keys)

        # --- search_near probes that must merge ingest + stable. ---
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(high))
        ordered = self.model_ordered_keys(high)
        for probe in ['big-00', 'chain', 'aaa', 'zzz', 'gen-9999', self.key(5)]:
            cursor.set_key(probe)
            ret = cursor.search_near()
            if not ordered:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
                continue
            self.assertTrue(ret == 0 or ret == 1 or ret == -1 or ret == wiredtiger.WT_NOTFOUND,
                self.fail_ctx('search_near(%r) odd ret %r' % (probe, ret)))
            if ret != wiredtiger.WT_NOTFOUND:
                landed = cursor.get_key()
                # The landed key must be a real visible key, and must be the
                # closest such key in the indicated direction.
                self.assertIn(landed, ordered,
                    self.fail_ctx('search_near(%r) landed on absent %r' % (probe, landed)))
                if probe in ordered:
                    self.assertEqual(landed, probe,
                        self.fail_ctx('search_near(%r) exact expected self, got %r' %
                            (probe, landed)))
                else:
                    # The nearest neighbour by string order.
                    import bisect
                    pos = bisect.bisect_left(ordered, probe)
                    cands = []
                    if pos < len(ordered):
                        cands.append(ordered[pos])      # next larger
                    if pos > 0:
                        cands.append(ordered[pos - 1])  # next smaller
                    self.assertIn(landed, cands,
                        self.fail_ctx('search_near(%r) landed %r not a neighbour of %r' %
                            (probe, landed, cands)))
            cursor.reset()
        self.session.rollback_transaction()

        # --- Cursor reset / reposition mid-iteration: walk a few keys, reset mid-stream,
        #     then a fresh full walk must still yield the complete ordered list. ---
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(high))
        cursor.reset()
        n = 0
        while cursor.next() == 0:
            n += 1
            if n == 3:
                break
        cursor.reset()                         # restart the walk mid-stream
        seen = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.assertEqual(seen, ordered,
            self.fail_ctx('reset-mid-iter engine=%r model=%r' % (seen, ordered)))
        self.session.rollback_transaction()

        # Leave the cutoff armed: post-cutoff content remains in ingest, so the
        # teardown's layered verification (an unarmed leader must have an empty
        # ingest) is satisfied.
        cursor.close()
        self.cleanStdout()

    # Repeated arm / abandon / re-arm cycles with readers, all stable-side so
    # abandoning leaves nothing stranded in ingest. Verifies the cutoff can move
    # through several values while concurrent readers keep seeing the model.
    def test_rearm_cycles(self):
        self.seed = 7
        random.seed(self.seed)
        self.model = {}
        self.oplog = []
        for i in range(1, self.nbase + 1):
            self.model_record(self.key(i), 20, 'base')

        cursor = self.create_with_base_content()
        session2 = self.conn.open_session('')
        cursor2 = session2.open_cursor(self.uri)

        ts = 30
        # Several arm/abandon cycles. Each cycle arms a cutoff well above the
        # timestamps it will use, so every write stays on the stable side and
        # abandoning (prepare_to_step_down=0) strands nothing in ingest.
        for cyc in range(5):
            cutoff = 100000 + cyc * 100000
            self.cleanStdout()
            self.conn.set_timestamp('prepare_to_step_down=' + self.timestamp_str(cutoff))
            self.assertTrace(r'stepdown: cutoff armed at %d' % cutoff)

            for _ in range(8):
                ts += 1
                self.session.begin_transaction()
                nops = random.randint(1, 4)
                writes = {}
                for _o in range(nops):
                    k = random.choice([self.key(i) for i in range(1, self.nbase + 1)] +
                                      ['rk-%02d' % j for j in range(6)])
                    if random.random() < 0.75:
                        v = 'c%d-%d' % (cyc, ts)
                        cursor[k] = v
                        writes[k] = v
                    else:
                        cursor.set_key(k)
                        cur = writes[k] if k in writes else self.model_visible(k, ts)
                        if cur is None:
                            self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
                        else:
                            self.assertEqual(cursor.remove(), 0)
                            writes[k] = TOMBSTONE
                self.session.commit_transaction(
                    'commit_timestamp=' + self.timestamp_str(ts))
                for (k, v) in writes.items():
                    self.model_record(k, ts, v)

            # A concurrent reader sees the committed model while armed.
            self.assert_reads_match(session2, cursor2, ts + 1,
                sorted(self.model.keys()))

            # Abandon the step-down: all content committed at ts < cutoff stays
            # on the stable side, so an unarmed leader sees the same thing.
            self.cleanStdout()
            self.conn.set_timestamp('prepare_to_step_down=0')
            self.assertTrace(r'stepdown: cutoff cleared \(step-down abandoned\)')
            self.assert_reads_match(self.session, cursor, ts + 1,
                sorted(self.model.keys()))
            self.assert_iteration_matches(cursor, ts + 1)

        cursor2.close()
        session2.close()
        cursor.close()
        self.cleanStdout()

    # The trace-watching helper, mirrored from async01.
    def assertTraces(self, patterns):
        out = self.readStdout(30000)
        for pat in patterns:
            self.assertTrue(re.search(pat, out),
                'expected trace %r not found in: %r' % (pat, out))
        self.cleanStdout()

    def assertTrace(self, pattern):
        self.assertTraces([pattern])
