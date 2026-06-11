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
# test_layered_cursor_stress.py
#
# Seed-driven stress test for layered cursors (src/cursor/cur_layered.c).
#
# Oracle: per connection (leader and follower) there are two tables in one session --
# a plain (non-layered) reference table ("ASC") and the layered table under test ("DSC").
# Every logical write is mirrored to both tables on both connections; every read op is run
# on the layered cursor and the reference cursor and the results (error code + key + value)
# are compared. The plain reference is real WiredTiger, so it is a correct oracle for
# read_timestamps / isolation / prepare with no modeling. The leader serves the layered
# table from stable only; the follower merges ingest + stable -- so the follower exercises
# the merge while both are checked against the same logical truth.
#
# Reproducibility: every run is driven by an integer seed (printed as SEED=<n>). Set the
# environment variable STRESS_SEED=<n> to replay a single seed. Every chosen event is
# appended to a per-seed trace file (path printed at start and on failure), flushed each
# step, so a failure is a self-contained record.

import os, random
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

def sign(n):
    return (n > 0) - (n < 0)

class EventTrace:
    # Append-only, flushed-each-line record of every chosen event for a seed.
    def __init__(self, path, header):
        self.path = path
        self._f = open(path, 'w')
        self._n = 0
        self._f.write('# %s\n' % header)
        self._f.flush()

    def log(self, event):
        self._f.write('%d: %s\n' % (self._n, event))
        self._f.flush()
        self._n += 1

    def note(self, text):
        self._f.write('# %s\n' % text)
        self._f.flush()

    def close(self):
        self._f.close()

class Node:
    # One connection's view: a layered table (DSC) and a plain reference table (ASC), each with
    # a single cursor used for BOTH reads and writes, all in one session. Reads and writes go
    # through the same cursors so the layered and reference cursors stay in lockstep and a write
    # leaves the cursor positioned per WT semantics (toward long-lived positioned chains).
    def __init__(self, conn, session, lay_uri, ref_uri):
        self.conn = conn
        self.session = session
        self.lay_uri = lay_uri
        self.ref_uri = ref_uri
        self.lay_c = session.open_cursor(lay_uri)
        self.ref_c = session.open_cursor(ref_uri)

    def reset_all(self):
        self.lay_c.reset()
        self.ref_c.reset()

    def close(self):
        self.lay_c.close()
        self.ref_c.close()

@disagg_test_class
class test_layered_cursor_stress(wttest.WiredTigerTestCase):
    conn_base_config = ',create,cache_size=1GB,statistics=(all),' \
                       'statistics_log=(wait=1,json=true,on_close=true),'

    # Candidate keys are spread with gaps so search_near targets can fall between keys.
    POOL = list(range(100, 1000, 10))

    disagg_storages = gen_disagg_storages('test_layered_cursor_stress', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    # --- cluster setup ---------------------------------------------------

    def setup_connections(self):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        self.ts = 0       # monotonic commit/stable timestamp
        self.wseq = 0     # monotonic write counter, for unique values
        self.dirty = False  # writes committed since the last checkpoint advance
        self.n_positional = 0  # positional update/remove ops applied (long-lived-chain guard)
        self.last_advance_ts = 0  # self.ts at the previous advance; oldest lags to here
        self.oldest_ts = 0   # current oldest_timestamp; floor for legal read_timestamps (C2)
        self.n_read_ts = 0   # as-of-past read transactions opened (read_timestamp guard)
        # Advancing to an unchanged checkpoint logs an expected WARNING.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')

    def make_nodes(self, tag):
        # The layered table must share a name across connections so the follower picks up
        # the leader's checkpoint; the plain reference tables are independent per connection.
        lay = 'layered:lcs_%s' % tag
        ref = 'table:lcs_ref_%s' % tag
        cfg = 'key_format=i,value_format=S'
        for s in (self.session, self.session_follow):
            s.create(lay, cfg)
            s.create(ref, cfg)
        self.live = {}     # logical key->value, for operation generation only (not the oracle)
        self.cur_pos = None  # key both cursor pairs are positioned on (None = unpositioned)
        self.in_txn = False       # an explicit transaction is open on both nodes' sessions
        self.txn_wrote = False    # the open txn has performed at least one write
        self.txn_read_ts = None   # if set, the open txn is a read-only as-of-T (read_timestamp)
        self.live_snapshot = None  # self.live as of begin, restored on rollback
        return [Node(self.conn, self.session, lay, ref),
                Node(self.conn_follow, self.session_follow, lay, ref)]

    # --- write protocol --------------------------------------------------

    def new_value(self, key):
        self.wseq += 1
        return 'v%d.%d' % (key, self.wseq)

    def _write_pair(self, n, key, value):
        # Bare write (no txn management) on node n's layered + reference cursors; compare codes.
        if value is None:
            n.lay_c.set_key(key); rl = n.lay_c.remove()
            n.ref_c.set_key(key); rr = n.ref_c.remove()
        else:
            n.lay_c.set_key(key); n.lay_c.set_value(value); rl = n.lay_c.insert()
            n.ref_c.set_key(key); n.ref_c.set_value(value); rr = n.ref_c.insert()
        # Under overwrite=true these are (0,0); the equality compare catches a layered-vs-
        # reference divergence once overwrite=false / prepare land.
        self.assertEqual(rl, rr,
            'write result differs layered=%r reference=%r (key=%r value=%r)' % (rl, rr, key, value))

    def mirror_write(self, nodes, key, value):
        # value=None means remove. On both connections (leader -> stable, follower -> ingest)
        # and the reference table. Inside an explicit transaction the write joins the open txn
        # (committed later by the commit op); otherwise it runs in its own timestamped txn.
        if self.in_txn:
            for n in nodes:
                self._write_pair(n, key, value)
            self.txn_wrote = True
        else:
            self.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                self._write_pair(n, key, value)
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts))
            self.dirty = True

    def _positional_pair(self, n, kind, value, key):
        if kind == 'pos_update':
            n.lay_c.set_value(value); rl = n.lay_c.update()
            n.ref_c.set_value(value); rr = n.ref_c.update()
        else:   # pos_remove
            rl = n.lay_c.remove()
            rr = n.ref_c.remove()
        self.assertEqual(rl, rr, 'positional %s result differs layered=%r reference=%r at key=%r'
                         % (kind, rl, rr, key))

    def apply_positional(self, nodes, kind, value):
        # Positional update/remove on the cursor's current key (self.cur_pos, which must be live).
        # Inside an explicit txn the positioning read and this write share the transaction, so
        # the cursor is genuinely positioned (KEY_INT|VALUE_INT) and we write DIRECTLY -- the
        # real iterate-and-delete-in-one-txn. In autocommit the positioning value is not valid
        # across the implicit txn boundary (WT-17796), so we re-search to re-establish position.
        key = self.cur_pos
        if self.in_txn:
            for n in nodes:
                self._positional_pair(n, kind, value, key)
            self.txn_wrote = True
        else:
            self.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                n.lay_c.set_key(key); rl_s = n.lay_c.search()
                n.ref_c.set_key(key); rr_s = n.ref_c.search()
                self.assertEqual((rl_s, rr_s), (0, 0),
                    'positional %s re-search failed layered=%r reference=%r key=%r' % (kind, rl_s, rr_s, key))
                self._positional_pair(n, kind, value, key)
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts))
            self.dirty = True
        self.n_positional += 1

    def _end_txn(self, nodes, commit):
        # Close the explicit transaction open on both nodes' sessions.
        if commit:
            if self.txn_wrote:
                self.ts += 1
                cts = 'commit_timestamp=' + self.timestamp_str(self.ts)
                for n in nodes:
                    n.session.commit_transaction(cts)
                self.dirty = True
            else:
                for n in nodes:
                    n.session.commit_transaction()
            # A successful commit keeps cursors positioned (cur_pos survives the txn switch),
            # except for an as-of-T read txn: that position is in a historical view and must not
            # anchor a write or chain in the latest-state autocommit world that follows.
            if self.txn_read_ts is not None:
                self.cur_pos = None
        else:
            for n in nodes:
                n.session.rollback_transaction()
            self.live = self.live_snapshot   # undo the txn's logical writes
            self.cur_pos = None              # rollback resets the session's cursors
        self.in_txn = False
        self.txn_wrote = False
        self.txn_read_ts = None
        self.live_snapshot = None            # consumed; the next begin takes a fresh snapshot

    def advance(self):
        # Fold the leader's stable into the follower's stable via a new checkpoint. Skip if
        # nothing changed since the last advance (avoids a redundant-checkpoint warning).
        #
        # stable moves to the latest commit, but oldest LAGS one advance behind it so the window
        # [oldest, latest] stays open for as-of-past reads (read_timestamp, C2). Pinning
        # oldest == stable would forbid reading anything below the latest commit. oldest is
        # monotonic (last_advance_ts only grows) and < stable (writes happened since).
        if not self.dirty:
            return
        oldest = max(1, self.last_advance_ts)
        self.conn.set_timestamp('oldest_timestamp=%s,stable_timestamp=%s'
                                % (self.timestamp_str(oldest), self.timestamp_str(self.ts)))
        self.oldest_ts = oldest
        self.last_advance_ts = self.ts
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        self.dirty = False

    def drain_ingest(self, node):
        # Evict the follower's ingest content so already-checkpointed keys fall through to
        # stable on later reads. The ingest is a single in-memory leaf page; forced eviction
        # reconciles it, dropping entries below the prune timestamp (already in stable) while
        # retaining fresher entries written since the last checkpoint advance. This is the
        # production lifecycle and is what actually drives the follower to read from stable.
        ingest_uri = 'file:' + node.lay_uri[len('layered:'):] + '.wt_ingest'
        ec = node.session.open_cursor(ingest_uri, None, 'debug=(release_evict)')
        try:
            for k in list(self.live):
                ec.set_key(k)
                if ec.search() == 0:
                    ec.reset()
        finally:
            ec.close()

    def follower_read_split(self):
        # Follower layered-cursor reads served from stable vs ingest. Uses only next/prev/search
        # (their stable branch is asserted in C to be a genuine stable-served read); search_near's
        # stable counter is impure (counts the current_cursor==NULL case, FIXME-WT-15545).
        c = self.session_follow.open_cursor('statistics:')
        try:
            g = lambda s: c[getattr(wiredtiger.stat.conn, s)][2]
            stable = g('layered_curs_next_stable') + g('layered_curs_prev_stable') + \
                g('layered_curs_search_stable')
            ingest = g('layered_curs_next_ingest') + g('layered_curs_prev_ingest') + \
                g('layered_curs_search_ingest')
            return stable, ingest
        finally:
            c.close()

    def assert_merge_exercised(self):
        # Guard against a degenerate oracle: the follower must read from stable a meaningful
        # fraction of the time, or the merge of two non-empty constituents is not being tested.
        # A floor (not just > 0) also catches a partial regression to ingest-only iteration.
        stable, ingest = self.follower_read_split()
        total = stable + ingest
        self.assertGreater(total, 0, 'no follower layered reads at all')
        self.assertGreaterEqual(stable * 10, total,
            'follower read from stable too rarely (%d/%d) -- merge not exercised' % (stable, total))

    # --- read application + comparison -----------------------------------

    def apply(self, cursor, op):
        # Returns normalized (ret, key, value, cmp).
        kind, arg = op
        if kind == 'reset':
            cursor.reset()
            return (0, None, None, None)
        if kind in ('next', 'prev'):
            ret = cursor.next() if kind == 'next' else cursor.prev()
            if ret == wiredtiger.WT_NOTFOUND:
                return (ret, None, None, None)
            return (0, cursor.get_key(), cursor.get_value(), None)
        cursor.set_key(arg)
        if kind == 'search':
            ret = cursor.search()
            if ret == wiredtiger.WT_NOTFOUND:
                return (ret, None, None, None)
            return (0, cursor.get_key(), cursor.get_value(), None)
        cmp = cursor.search_near()
        if cmp == wiredtiger.WT_NOTFOUND:
            return (wiredtiger.WT_NOTFOUND, None, None, None)
        return (0, cursor.get_key(), cursor.get_value(), cmp)

    def compare_read(self, op, node, trace):
        # Layered (DSC) cursor vs reference (ASC) cursor, in the same session/snapshot.
        r_lay = self.apply(node.lay_c, op)
        r_ref = self.apply(node.ref_c, op)
        if op[0] == 'search_near':
            self.compare_search_near(op, node, r_lay, r_ref, trace)
        elif r_lay != r_ref:
            self.fail_mismatch(op, node, r_lay, r_ref, trace, 'result differs')
        return r_lay

    def compare_search_near(self, op, node, r_lay, r_ref, trace):
        (retl, kl, vl, cl) = r_lay
        (retr, kr, vr, cr) = r_ref
        if retl == wiredtiger.WT_NOTFOUND or retr == wiredtiger.WT_NOTFOUND:
            if retl != retr:
                self.fail_mismatch(op, node, r_lay, r_ref, trace, 'one side NOTFOUND')
            return
        search_key = op[1]
        # The layered cmp sign must agree with the returned key vs the search key (A11): 0 on
        # exact match, <0 if the returned key is smaller, >0 if larger.
        if sign(cl) != sign(kl - search_key):
            self.fail_mismatch(op, node, r_lay, r_ref, trace, 'layered search_near cmp sign wrong')
        if kl == kr:
            if sign(cl) != sign(cr) or vl != vr:
                self.fail_mismatch(op, node, r_lay, r_ref, trace, 'same key, differing cmp/value')
            return
        # Different immediate neighbours: must bracket the search key and be adjacent. Step
        # the reference cursor by exactly one onto the layered cursor's key to re-sync.
        lo, hi = sorted((kl, kr))
        if not (lo < search_key < hi):
            self.fail_mismatch(op, node, r_lay, r_ref, trace, 'neighbours do not bracket key')
        stepped = node.ref_c.next() if kr < kl else node.ref_c.prev()
        if stepped != 0 or node.ref_c.get_key() != kl or node.ref_c.get_value() != vl:
            self.fail_mismatch(op, node, r_lay, r_ref, trace, 'neighbours not adjacent / value')

    def fail_mismatch(self, op, node, r_lay, r_ref, trace, reason):
        self.fail('\n'.join([
            'layered-vs-reference mismatch on %s node: %s' % (node.conn == self.conn and 'leader' or 'follower', reason),
            'failing op: %r' % (op,),
            'layered:   %r' % (r_lay,),
            'reference: %r' % (r_ref,),
            'trace file: %s' % trace.path]))

    # --- verification ----------------------------------------------------

    def scan(self, cursor, forward):
        cursor.reset()
        out = []
        while True:
            ret = cursor.next() if forward else cursor.prev()
            if ret == wiredtiger.WT_NOTFOUND:
                break
            out.append((cursor.get_key(), cursor.get_value()))
        return out

    def verify(self, nodes, trace):
        # Each node: layered full scan must equal its reference; and the leader's layered
        # view must equal the follower's layered view (same logical data).
        per_node = []
        for n in nodes:
            lay = self.scan(n.lay_c, True)
            ref = self.scan(n.ref_c, True)
            if lay != ref:
                self.fail('full-scan layered != reference (trace %s)\nlayered=%r\nref=%r'
                          % (trace.path, lay, ref))
            per_node.append(lay)
        if per_node[0] != per_node[1]:
            self.fail('leader layered scan != follower layered scan (trace %s)' % trace.path)

    # --- operation generation -------------------------------------------

    def pick_op(self, rnd, allow_writes):
        # Favour long position-holding chains: when the cursor is positioned, weight
        # next/prev and positional update/remove heavily; keep position-resetting ops
        # (set_key put/remove, reset) rare. advance/evict punctuate chains (checkpoint
        # lifecycle). When unpositioned, use the ops that re-establish a position.
        #
        # Transactions: 'begin' is only legal in autocommit; 'commit'/'rollback' only with an
        # open txn; advance/evict (checkpoint lifecycle) only in autocommit. Inside a txn,
        # positional writes are the genuine same-transaction iterate-and-delete (apply_positional).
        # A read_timestamp (as-of-past) txn is read-only -- no writes inside it (self.txn_read_ts).
        positioned = self.cur_pos is not None and self.cur_pos in self.live
        if not allow_writes:
            kinds   = ['search', 'search_near', 'next', 'prev', 'reset']
            weights = [15, 12, 20, 20, 6]
        elif self.in_txn and self.txn_read_ts is not None:
            # As-of-T read-only txn: reads chain across a historical snapshot, ended by
            # commit/rollback. cur_pos may sit on a key absent from current self.live (it was
            # live at T), so weight next/prev on raw positioned-ness, not live membership.
            if self.cur_pos is not None:
                kinds   = ['next', 'prev', 'search', 'search_near', 'reset', 'commit', 'rollback']
                weights = [24, 24, 8, 8, 2, 16, 6]
            else:
                kinds   = ['search', 'search_near', 'next', 'prev', 'reset', 'commit', 'rollback']
                weights = [14, 11, 20, 20, 2, 16, 6]
        elif self.in_txn:
            if positioned:
                kinds   = ['next', 'prev', 'pos_update', 'pos_remove', 'search', 'search_near',
                           'put', 'remove', 'reset', 'commit', 'rollback']
                weights = [20, 20, 14, 9, 6, 6, 3, 2, 1, 12, 5]
            else:
                kinds   = ['search', 'search_near', 'next', 'prev', 'put', 'remove', 'reset',
                           'commit', 'rollback']
                weights = [14, 11, 18, 18, 12, 3, 2, 12, 5]
        else:
            if positioned:
                kinds   = ['next', 'prev', 'pos_update', 'pos_remove', 'search', 'search_near',
                           'advance', 'evict', 'put', 'remove', 'reset', 'begin']
                weights = [22, 22, 12, 7, 6, 6, 3, 4, 2, 1, 1, 5]
            else:
                kinds   = ['search', 'search_near', 'next', 'prev', 'put', 'advance', 'evict',
                           'reset', 'begin']
                weights = [16, 12, 20, 20, 12, 3, 4, 2, 5]
        kind = rnd.choices(kinds, weights=weights, k=1)[0]
        if kind == 'put':
            return ('put', rnd.choice(self.POOL))
        if kind == 'remove':
            return ('remove', rnd.choice(list(self.live))) if self.live else ('reset', None)
        if kind == 'begin':
            # Half the time, when a past window exists, open a read-only as-of-T transaction;
            # the read_timestamp is any point in [oldest, latest] -- both tables must agree on
            # that historical view. Otherwise a normal read-write transaction. The `oldest_ts>=1`
            # gate is load-bearing: it keeps randint off timestamp 0 (an invalid read_timestamp).
            if self.oldest_ts >= 1 and self.ts > self.oldest_ts and rnd.random() < 0.5:
                return ('begin', rnd.randint(self.oldest_ts, self.ts))
            return ('begin', None)
        if kind in ('evict', 'commit', 'rollback',
                    'advance', 'next', 'prev', 'reset', 'pos_update', 'pos_remove'):
            return (kind, None)
        return (kind, self.pick_search_key(rnd))

    def pick_search_key(self, rnd):
        r = rnd.random()
        if self.live and r < 0.5:
            return rnd.choice(list(self.live))
        if r < 0.8:
            return rnd.choice(self.POOL)
        return rnd.choice(self.POOL) + 5   # off-grid gap, always absent

    # --- driver ----------------------------------------------------------

    def open_trace(self, seed, tag):
        path = os.path.join(os.getcwd(), 'stress_trace_%s_%d.txt' % (tag, seed))
        self.pr('SEED=%d trace=%s' % (seed, path))
        return EventTrace(path, 'test_layered_cursor_stress seed=%d tag=%s' % (seed, tag))

    def run_seed(self, seed, tag, n_ops, allow_writes):
        rnd = random.Random(seed)
        trace = self.open_trace(seed, tag)
        nodes = self.make_nodes(tag)
        leader, follower = nodes
        self.cur_pos = None
        try:
            for _ in range(n_ops):
                op = self.pick_op(rnd, allow_writes)
                trace.log('%s %r' % (op[0], op[1]))
                kind = op[0]
                if kind == 'put':
                    # set_key-based write: clears the cursor position.
                    v = self.new_value(op[1]); self.mirror_write(nodes, op[1], v)
                    self.live[op[1]] = v
                    self.cur_pos = None
                elif kind == 'remove':
                    self.mirror_write(nodes, op[1], None); self.live.pop(op[1], None)
                    self.cur_pos = None
                elif kind == 'pos_update':
                    # Positional write: keeps the cursor on self.cur_pos.
                    v = self.new_value(self.cur_pos)
                    self.apply_positional(nodes, 'pos_update', v)
                    self.live[self.cur_pos] = v
                elif kind == 'pos_remove':
                    # Removes the current key; the cursor stays on the (now deleted) slot.
                    self.apply_positional(nodes, 'pos_remove', None)
                    self.live.pop(self.cur_pos, None)
                elif kind == 'begin':
                    # Open one explicit transaction per node; subsequent writes join it and
                    # commit atomically at the commit op. Snapshot logical state for rollback.
                    # op[1] None -> read-write txn; an int -> read-only as-of-T (read_timestamp):
                    # both tables must agree on the historical view, validating the layered
                    # cursor's read_timestamp merge of stable + ingest.
                    read_ts = op[1]
                    cfg = '' if read_ts is None else \
                        'read_timestamp=' + self.timestamp_str(read_ts)
                    for n in nodes:
                        n.session.begin_transaction(cfg)
                    self.in_txn = True
                    self.txn_wrote = False
                    self.txn_read_ts = read_ts
                    if read_ts is not None:
                        self.n_read_ts += 1
                    # Snapshot for rollback. An as-of-T txn never writes, so the restore is a
                    # no-op for it, but keep it unconditional so rollback has a value to restore.
                    self.live_snapshot = dict(self.live)
                    # The cursor stays physically positioned so next/prev keep iterating across
                    # the switch, but a positional WRITE must be re-established by a read inside
                    # this txn -- a positional write off a pre-txn position is the cross-txn
                    # positioned-remove (WT-17796). Clear the generator's position to enforce that.
                    self.cur_pos = None
                elif kind == 'commit':
                    self._end_txn(nodes, commit=True)
                elif kind == 'rollback':
                    self._end_txn(nodes, commit=False)
                elif kind in ('advance', 'evict'):
                    # Checkpoint ops release any pinned snapshot first (reset cursors).
                    for n in nodes:
                        n.reset_all()
                    self.advance()
                    if kind == 'evict':
                        self.drain_ingest(follower)
                    self.cur_pos = None
                else:   # read op: track the resulting position for the next chain step
                    lead, foll = (self.compare_read(op, nodes[0], trace),
                                  self.compare_read(op, nodes[1], trace))
                    # cur_pos anchors a positional write, which inside a txn operates on each
                    # cursor's CURRENT position directly. That is only valid when leader and
                    # follower ended on the same key: search_near may legitimately land them on
                    # different (both valid) neighbours, so don't anchor a positional write there.
                    if lead[0] == 0 and foll[0] == 0 and lead[1] == foll[1]:
                        self.cur_pos = lead[1]
                    else:
                        self.cur_pos = None
            if self.in_txn:
                # Close any transaction left open at the end of the chain before verifying.
                self._end_txn(nodes, commit=True)
            self.verify(nodes, trace)
        finally:
            if self.in_txn:
                # A failure fired mid-transaction. Roll it back so teardown is clean, but never
                # let this mask the original error.
                for n in nodes:
                    try:
                        n.session.rollback_transaction()
                    except Exception:
                        pass
                self.in_txn = False
            for n in nodes:
                n.close()
            trace.close()

    def replaying_single_seed(self):
        return bool(os.environ.get('STRESS_SEED'))

    def seeds_for(self, default_seeds):
        env = os.environ.get('STRESS_SEED')
        return [int(env)] if env else default_seeds

    # --- tests -----------------------------------------------------------

    def test_smoke(self):
        # Short seeded run with writes, starting from empty tables.
        self.setup_connections()
        self.run_seed(seed=12345, tag='smoke', n_ops=80, allow_writes=True)

    def test_read_only(self):
        # Build a genuine merge state, then many read-only op sequences over it. Batch A is
        # checkpointed and DRAINED from the follower ingest (so it is stable-only there); batch
        # B is written fresh (follower ingest). The follower must therefore merge stable(A) +
        # ingest(B) -- this also verifies the mixed temporal case (B survives the drain while A
        # falls to stable), which the eviction investigation did not cover.
        self.setup_connections()
        nodes = self.make_nodes('ro')
        leader, follower = nodes
        for k in range(100, 200, 20):           # batch A: 100,120,140,160,180
            self.mirror_write(nodes, k, self.new_value(k)); self.live[k] = 'x'
        self.advance()                          # checkpoint 1 (contains A)
        for k in range(110, 200, 20):           # batch B: 110,130,... -> follower ingest
            self.mirror_write(nodes, k, self.new_value(k)); self.live[k] = 'x'
        self.advance()                          # checkpoint 2; A is now in an older checkpoint
        self.drain_ingest(follower)             # prunes A (older ckpt) from ingest -> stable-only
        for n in nodes:
            n.reset_all()
        # Merge correctness: the follower's merged scan must equal its reference (B not lost).
        self.assertEqual(self.scan(follower.lay_c, True), self.scan(follower.ref_c, True))
        trace = self.open_trace(0, 'ro')
        try:
            for seed in self.seeds_for(range(10)):
                rnd = random.Random(seed)
                trace.note('seed=%d' % seed)
                for _ in range(300):
                    op = self.pick_op(rnd, allow_writes=False)
                    trace.log('%s %r' % (op[0], op[1]))
                    for n in nodes:
                        self.compare_read(op, n, trace)
            self.verify(nodes, trace)
        finally:
            for n in nodes:
                n.close()
            trace.close()
        if not self.replaying_single_seed():
            # The follower genuinely read from stable (A keys), not ingest-only. (Aggregate
            # coverage guard; skipped under a single-seed replay -- see test_random.)
            self.assert_merge_exercised()

    def test_random(self):
        # Mixed read/write/advance/evict sequences, fresh tables per seed, start empty.
        self.setup_connections()
        for seed in self.seeds_for(range(10)):
            self.run_seed(seed=seed, tag='r%d' % seed, n_ops=300, allow_writes=True)
        if not self.replaying_single_seed():
            # Aggregate coverage guards over the full multi-seed run -- not invariants of any
            # single chain, so skip them under a pinned-seed replay (STRESS_SEED), whose purpose
            # is reproducing one specific sequence, not exercising the whole coverage surface.
            # Guard against a degenerate oracle: the follower must actually read from stable.
            self.assert_merge_exercised()
            # Long-lived chains must actually run positional update/remove (not all gated out).
            self.assertGreater(self.n_positional, 0, 'no positional update/remove ops were exercised')
            # As-of-past (read_timestamp) read transactions must actually be exercised.
            self.assertGreater(self.n_read_ts, 0, 'no read_timestamp (as-of-past) txns were exercised')

    def test_scenario_checkpoint_delete_visible(self):
        # Regression: after deletes are folded into a new checkpoint, a follower cursor with
        # no pinned snapshot reflects the deletion via both scan and point search. (A pinned
        # snapshot -- held cursor / explicit txn -- legitimately keeps the old view; that is
        # correct read-committed semantics, not a bug. See finding-stale-checkpoint-cursor.)
        self.setup_connections()
        nodes = self.make_nodes('ckpt')
        try:
            for k in (100, 110, 120, 130):
                self.mirror_write(nodes, k, 'v%d' % k)
            self.advance()
            for k in (110, 120):
                self.mirror_write(nodes, k, None)
            self.advance()
            # Fresh follower session + single cursor: nothing pins the snapshot.
            rs = self.conn_follow.open_session('')
            c = rs.open_cursor('layered:lcs_ckpt')
            self.assertEqual(self.scan(c, True), [(100, 'v100'), (130, 'v130')])
            for k, exp in [(100, 0), (110, wiredtiger.WT_NOTFOUND),
                           (120, wiredtiger.WT_NOTFOUND), (130, 0)]:
                c.reset(); c.set_key(k)
                self.assertEqual(c.search(), exp)
            c.close(); rs.close()
        finally:
            for n in nodes:
                n.close()

    def test_scenario_read_timestamp_history(self):
        # Regression for the read_timestamp merge (C2): a key overwritten across two checkpoints,
        # with the older version in stable's history and the ingest drained. Reading at the old
        # commit's timestamp must reconstruct the OLD value on the follower's merged cursor, and
        # at the new commit's timestamp the NEW value -- on both the layered and reference tables.
        self.setup_connections()
        nodes = self.make_nodes('rts')
        leader, follower = nodes
        try:
            self.mirror_write(nodes, 100, 'old'); self.live[100] = 'x'
            ts_old = self.ts
            self.advance()                  # checkpoint 1: 100='old'
            self.mirror_write(nodes, 100, 'new'); self.live[100] = 'x'
            ts_new = self.ts
            self.advance()                  # checkpoint 2: 100='new'; oldest now lags to ts_old
            self.drain_ingest(follower)     # prune already-stable ingest -> read from stable+history
            for n in nodes:
                n.reset_all()
            for ts, exp in [(ts_old, 'old'), (ts_new, 'new')]:
                for n in nodes:
                    n.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
                    for cur in (n.lay_c, n.ref_c):
                        cur.set_key(100)
                        self.assertEqual(cur.search(), 0, 'as-of-%d search on %s' % (ts, n.lay_uri))
                        self.assertEqual(cur.get_value(), exp,
                                         'as-of-%d value on %s' % (ts, n.lay_uri))
                        cur.reset()
                    n.session.rollback_transaction()
        finally:
            for n in nodes:
                n.close()
