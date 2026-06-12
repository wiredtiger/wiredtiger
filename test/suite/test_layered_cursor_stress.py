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
# Oracle: per connection (leader and follower), one session holds a plain reference table (asc)
# and the layered table under test (dsc). Every write is mirrored to both; every read is run on
# both cursors and the (error code, key, value) compared. The plain reference is real WiredTiger,
# so it is a correct oracle for read_timestamps / isolation / prepare with no modeling. The leader
# serves the layered table from stable only; the follower merges ingest + stable, so the follower
# exercises the merge against the same logical truth.
#
# The seed set is fixed, so a run is deterministic and a failure repeats on re-run (the failing
# seed prints as SEED=<n>). Every chosen event is appended to a per-seed trace file, flushed each
# step; to dig into one seed, run it in a throwaway test calling run_sequence().

import os, random
from dataclasses import dataclass, field
from enum import Enum
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

class Txn(Enum):
    # The generator's current transaction context. Only NO and SNAPSHOT permit writes; READ_COMMITTED
    # / READ_UNCOMMITTED reject writes (txn_inline.h ~2112) and READ_TIMESTAMP is an as-of-past read.
    # The value is the WiredTiger isolation= config string where one applies.
    NO = 'no'                              # autocommit -- no explicit transaction
    SNAPSHOT = 'snapshot'                  # read-write snapshot transaction
    READ_COMMITTED = 'read-committed'      # read-only
    READ_UNCOMMITTED = 'read-uncommitted'  # read-only
    READ_TIMESTAMP = 'read-timestamp'      # snapshot isolation + an as-of-past read; read-only

def write_allowed(txn):
    return txn in (Txn.NO, Txn.SNAPSHOT)

# --- workload shape --------------------------------------------------------
# Each operation is an Op (built in _build_ops()) pairing an op method -- referenced directly, never
# dispatched by string -- with its weight and legality tags. A test builds a Weights and passes it
# in; _build_ops copies each named field onto the Op that uses it (Op(self.op_next, weights.next)),
# so pick_op samples plain Op rows with no lookup. Adding an op = one op_<name> method + one Op row +
# one Weights field. TxnBeginWeights is the one sub-distribution not in the top-level pool: op_begin
# reads it directly to pick the transaction flavour it opens.
# TODO(workload-tuning): rough first pass (reads dominate; breaks/txn/scenarios rare). Once the long
# run lands, tune to drive the follower stable-read fraction up (see DEV_ONLY_assert_merge_exercised)
# and decide whether a derived break-frequency knob should return.

@dataclass(frozen=True)
class TxnBeginWeights:
    # op_begin's transaction-flavour sub-distribution. snapshot is read-write; the rest are read-only
    # (read_timestamp = snapshot + an as-of-past read, falling back to snapshot with no past window).
    snapshot: int = 72
    read_committed: int = 16
    read_uncommitted: int = 12
    read_timestamp: int = 30

@dataclass(frozen=True)
# Q: Question for the future. Some weights should be less than 1% probability (like evict or advance) - probably 0.1% or even 0.01% for the long running tests Should we just make other weights bigger
class Weights:
    # Position-holding ops (reads + positional writes) carry the big weights so chains stay long and
    # the cursor is usually positioned -- the heart of the test. With no break gate, a position-
    # breaking op's raw weight is its break frequency.
    next: int = 40
    prev: int = 40
    search: int = 12
    search_near: int = 10
    pos_update: int = 14
    pos_remove: int = 8
    put: int = 6
    remove: int = 2
    reset: int = 2
    verify: int = 4
    advance: int = 6
    evict: int = 6
    begin: int = 8
    commit: int = 6
    rollback: int = 2
    txn_begin: TxnBeginWeights = field(default_factory=TxnBeginWeights)

@dataclass(frozen=True)
class Op:
    fn: object                    # the op method, called as fn(nodes, rnd, trace); the dispatch identity
    weight: int                   # relative frequency among the legal ops at each step
    needs_position: bool = False  # cursor must be positioned on a live key (positional writes)
    needs_live: bool = False      # at least one live key must exist (remove by key)
    is_write: bool = False        # a logical write (illegal in a read-only transaction)
    autocommit_only: bool = False  # only with no open txn (begin / advance / evict)
    in_txn_only: bool = False     # only with an open txn (commit / rollback)

class EventTrace:
    # Append-only, flushed-per-line record of every chosen event for a seed.
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

    def close(self):
        self._f.close()

class Node:
    # One connection's view: a layered table (dsc) and a plain reference table (asc), one cursor each
    # in one session, used for both reads and writes. Sharing the cursor keeps layered and reference
    # in lockstep and leaves the cursor positioned after a write (toward long-lived positioned chains).
    def __init__(self, conn, session, dsc_uri, asc_uri):
        self.conn = conn
        self.session = session
        self.dsc_uri = dsc_uri
        self.asc_uri = asc_uri
        self.dsc_c = session.open_cursor(dsc_uri)
        self.asc_c = session.open_cursor(asc_uri)

    def reset_all(self):
        self.dsc_c.reset()
        self.asc_c.reset()

    def close(self):
        self.dsc_c.close()
        self.asc_c.close()

class State:
    # The model the generator reasons about, never the oracle -- it only drives op generation.
    # Connection-global fields (timestamps, coverage counters) persist across sequences in a
    # multi-seed run; new_sequence() resets the per-sequence fields for each fresh set of tables.
    def __init__(self):
        self.ts = 0              # monotonic commit/stable timestamp
        self.wseq = 0            # monotonic write counter, for unique values
        self.dirty = False       # writes committed since the last checkpoint advance
        self.oldest_ts = 0       # current oldest_timestamp; floor for legal read_timestamps (C2)
        self.last_advance_ts = 0  # self.ts at the previous advance; oldest lags to here
        self.n_positional = 0    # positional update/remove ops applied (long-lived-chain guard)
        self.n_read_ts = 0       # as-of-past read transactions opened (read_timestamp guard)
        self.n_iso_rc = 0        # read-committed transactions opened (isolation guard, C3)
        self.n_iso_ru = 0        # read-uncommitted transactions opened (isolation guard, C3)
        self.n_verify = 0        # full-table verify ops run (op_verify guard)
        self.new_sequence()

    def new_sequence(self):
        self.py_table = {}             # logical key->value, for op generation only
        self.cur_pos = None            # key both cursor pairs are positioned on (None = unpositioned)
        self.txn = Txn.NO              # current transaction context (NO = autocommit)
        self.txn_wrote = False         # the open txn has performed at least one write
        self.txn_read_ts = None        # the as-of timestamp when txn is READ_TIMESTAMP, else None
        self.py_table_snapshot = None  # py_table as of begin, restored on rollback

@disagg_test_class
class test_layered_cursor_stress(wttest.WiredTigerTestCase):
    conn_base_config = ',create,cache_size=1GB,statistics=(all),' \
                       'statistics_log=(wait=1,json=true,on_close=true),'

    # Q: Does POOL mean that we always have not more than 100 keys? Not bad, but should we extend it to 1000?
    # Candidate keys are spread with gaps so search_near targets can fall between keys.
    POOL = list(range(100, 1000, 10))

    disagg_storages = gen_disagg_storages('test_layered_cursor_stress', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    # --- cluster setup ---------------------------------------------------

    def setup_connections(self, weights):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        self.state = State()
        self.weights = weights
        self.ops = self._build_ops(weights)   # the workload table (Op rows -> op methods)
        self.DEV_ONLY_validate_ops()          # the table must match the op_* methods exactly
        # Advancing to an unchanged checkpoint logs an expected WARNING.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')

    def _build_ops(self, weights):
        # One Op row per op, pairing the op method (the dispatch identity) with its legality tags and
        # the weight copied from the Weights field it belongs to, so pick_op samples plain rows.
        return [
            Op(self.op_next,        weights.next),
            Op(self.op_prev,        weights.prev),
            Op(self.op_search,      weights.search),
            Op(self.op_search_near, weights.search_near),
            Op(self.op_pos_update,  weights.pos_update, needs_position=True, is_write=True),
            Op(self.op_pos_remove,  weights.pos_remove, needs_position=True, is_write=True),
            Op(self.op_put,         weights.put,        is_write=True),
            # Q: Why remove requires need_live? If it set's the key from scratch it breaks the position, so even if we try to remove unexisting key we can just check that both asc and dsc removes not_found. Maybe we can create a separate remove weights structure and say that we remove existing keys here
            Op(self.op_remove,      weights.remove,     is_write=True, needs_live=True),
            Op(self.op_reset,       weights.reset),
            # Q: op_verify, op_advance and op_evict should be scen_advance, scen_verify, scen_evic, since they are not operations
            Op(self.op_verify,      weights.verify),
            # Q: autocommit_only and in_txn_only should be no_txn, in_txn that are true by default and we turn it to false where needed
            Op(self.op_advance,     weights.advance,    autocommit_only=True),
            Op(self.op_evict,       weights.evict,      autocommit_only=True),
            # Q: Rename to op_txn_begin
            Op(self.op_begin,       weights.begin,      autocommit_only=True),
            # Q: I think that op_commit and op_rollback should be a part of op_txn_begin(), so when we call it first time, we start transaction, when we call it second time, we either roll it back or commit (90/10). Weight for them should be moved to txn weights as well.
            Op(self.op_commit,      weights.commit,     in_txn_only=True),
            Op(self.op_rollback,    weights.rollback,   in_txn_only=True),
        ]

    def make_nodes(self, tag):
        # The layered table must share a name across connections so the follower picks up
        # the leader's checkpoint; the plain reference tables are independent per connection.
        dsc = 'layered:lcs_dsc_%s' % tag
        asc = 'table:lcs_asc_%s' % tag
        cfg = 'key_format=i,value_format=S'
        for s in (self.session, self.session_follow):
            s.create(dsc, cfg)
            s.create(asc, cfg)
        self.state.new_sequence()
        return [Node(self.conn, self.session, dsc, asc),
                Node(self.conn_follow, self.session_follow, dsc, asc)]

    # --- write protocol --------------------------------------------------

    def new_value(self, key):
        self.state.wseq += 1
        return 'v%d.%d' % (key, self.state.wseq)

    # Q: What's the reason to have both remove() and insert() a part of this function consider do it separately in op_remove() and op_insert() to avoid creating function with shared repsonsibilities
    def _write_pair(self, n, key, value):
        # Bare write (no txn management) on node n's layered + reference cursors; compare codes.
        # value=None means remove.
        if value is None:
            n.dsc_c.set_key(key); ret_dsc = n.dsc_c.remove()
            n.asc_c.set_key(key); ret_asc = n.asc_c.remove()
        else:
            n.dsc_c.set_key(key); n.dsc_c.set_value(value); ret_dsc = n.dsc_c.insert()
            n.asc_c.set_key(key); n.asc_c.set_value(value); ret_asc = n.asc_c.insert()
        # Under overwrite=true these are (0,0); the compare catches a divergence once
        # overwrite=false / prepare land.
        self.assertEqual(ret_dsc, ret_asc,
            'write result differs layered=%r reference=%r (key=%r value=%r)' % (ret_dsc, ret_asc, key, value))

    # Q: It seems like all the writes have the same logic around them: I mean is selft.state.txn is not Txn.NO ..., else begin_txn, commit txn. I think we can also create a separate function to do the same chain of ops on the given session, cursor, node and then assert the cursors after it + take a op label to print it in case of failure.
    def mirror_write(self, nodes, key, value):
        # Write to both connections (leader -> stable, follower -> ingest) and both reference tables.
        # Inside an explicit txn the write joins it (committed later); otherwise it runs in its own
        # timestamped txn. value=None means remove.
        if self.state.txn is not Txn.NO:
            for n in nodes:
                self._write_pair(n, key, value)
            self.state.txn_wrote = True
        else:
            self.state.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                self._write_pair(n, key, value)
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.state.ts))
            self.state.dirty = True

    def _positional(self, nodes, do):
        # Run a positional update/remove (do(cursor) -> code) on the cursor's current key (cur_pos,
        # which must be live). Inside an explicit txn the positioning read and this write share the
        # transaction, so the cursor is genuinely positioned and we write DIRECTLY -- the real
        # iterate-and-delete-in-one-txn. In autocommit the position is not valid across the implicit
        # txn boundary (WT-17796), so we re-search to re-establish it.
        key = self.state.cur_pos
        if self.state.txn is not Txn.NO:
            for n in nodes:
                ret_dsc = do(n.dsc_c); ret_asc = do(n.asc_c)
                self.assertEqual(ret_dsc, ret_asc, 'positional result differs layered=%r reference=%r at key=%r'
                                 % (ret_dsc, ret_asc, key))
            self.state.txn_wrote = True
        else:
            self.state.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                n.dsc_c.set_key(key); search_dsc = n.dsc_c.search()
                n.asc_c.set_key(key); search_asc = n.asc_c.search()
                self.assertEqual((search_dsc, search_asc), (0, 0),
                    'positional re-search failed layered=%r reference=%r key=%r' % (search_dsc, search_asc, key))
                ret_dsc = do(n.dsc_c); ret_asc = do(n.asc_c)
                self.assertEqual(ret_dsc, ret_asc, 'positional result differs layered=%r reference=%r at key=%r'
                                 % (ret_dsc, ret_asc, key))
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.state.ts))
            self.state.dirty = True
        self.state.n_positional += 1

    def _end_txn(self, nodes, commit):
        # close the explicit transaction open on both nodes' sessions.
        if commit:
            if self.state.txn_wrote:
                self.state.ts += 1
                cts = 'commit_timestamp=' + self.timestamp_str(self.state.ts)
                for n in nodes:
                    n.session.commit_transaction(cts)
                self.state.dirty = True
            else:
                for n in nodes:
                    n.session.commit_transaction()

            # Q: Can we do?
            # cts = None
            # if self.state.txn_wrote:
            #    self.state.ts += 1
            #    self.state.dirty = True
            # for n in nodes:
            #    n.session.commit_transaction()

            # Q: Can you explain me once more why do we need it in simple words with an example?
            # A successful commit keeps cursors positioned (cur_pos survives), except an as-of-T read
            # txn: resuming a latest iteration from a held historical position lets the layered cursor
            # pin its stable constituent across the snapshot change and diverge from the reference
            # (the Q2 family, ruled not-a-bug -- a fresh read agrees), so reset those cursors.
            # TODO(pin-reset): RC/RU read-only txns also hold cursors across commit but resume in a
            # compatible (latest) view, so no pin divergence has been seen; extend the reset to them
            # only if one appears.
            if self.state.txn_read_ts is not None:
                for n in nodes:
                    n.reset_all()
                self.state.cur_pos = None
        else:
            for n in nodes:
                n.session.rollback_transaction()
            self.state.py_table = self.state.py_table_snapshot   # undo the txn's logical writes
            self.state.cur_pos = None              # rollback resets the session's cursors
        self.state.txn = Txn.NO
        self.state.txn_wrote = False
        self.state.txn_read_ts = None
        self.state.py_table_snapshot = None

    def advance(self):
        # Fold the leader's stable into the follower's via a new checkpoint; skip if nothing changed.
        # stable moves to the latest commit, but oldest LAGS one advance behind so the window
        # [oldest, latest] stays open for as-of-past reads (pinning oldest == stable would forbid
        # reading below the latest commit). oldest is monotonic and < stable.
        # Q: what's the problem with checkpointing even if nothing has changed? it might be interesting from the testing point of view.
        if not self.state.dirty:
            return
        oldest = max(1, self.state.last_advance_ts)
        self.conn.set_timestamp('oldest_timestamp=%s,stable_timestamp=%s'
                                % (self.timestamp_str(oldest), self.timestamp_str(self.state.ts)))
        self.state.oldest_ts = oldest
        self.state.last_advance_ts = self.state.ts
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)
        self.state.dirty = False

    def drain_ingest(self, node):
        # Force-evict the follower's ingest leaf so checkpointed keys fall through to stable on later
        # reads: reconciliation drops entries below the prune timestamp (already in stable) and keeps
        # fresher ones. This is the production lifecycle that drives the follower to read from stable.
        ingest_uri = 'file:' + node.dsc_uri[len('layered:'):] + '.wt_ingest'
        evict_cursor = node.session.open_cursor(ingest_uri, None, 'debug=(release_evict)')
        try:
            for k in list(self.state.py_table):
                evict_cursor.set_key(k)
                if evict_cursor.search() == 0:
                    evict_cursor.reset()
        finally:
            evict_cursor.close()

    def DEV_ONLY_follower_read_split(self):
        # Follower layered reads served from stable vs ingest. Uses only next/prev/search; search_near's
        # stable counter is impure (counts the current_cursor==NULL case, FIXME-WT-15545).
        stat_cursor = self.session_follow.open_cursor('statistics:')
        try:
            g = lambda s: stat_cursor[getattr(wiredtiger.stat.conn, s)][2]
            stable = g('layered_curs_next_stable') + g('layered_curs_prev_stable') + \
                g('layered_curs_search_stable')
            ingest = g('layered_curs_next_ingest') + g('layered_curs_prev_ingest') + \
                g('layered_curs_search_ingest')
            return stable, ingest
        finally:
            stat_cursor.close()

    def DEV_ONLY_assert_merge_exercised(self):
        # Guard against a degenerate oracle: the follower must read from stable sometimes, or the
        # merge of two non-empty constituents is not tested at all.
        # TODO(merge-coverage): the stable-read fraction is only ~2-9% in the random run (writes keep
        # keys in ingest and the drain rarely catches a stable-and-unrewritten key before it is read),
        # so the floor is an INTERIM 1%. Real fix: a forced-eviction scenario op + a ~300k-op run to
        # exercise the stable path heavily, then restore a meaningful floor.
        stable, ingest = self.DEV_ONLY_follower_read_split()
        total = stable + ingest
        self.assertGreater(total, 0, 'no follower layered reads at all')
        self.assertGreaterEqual(stable * 100, total,
            'follower read from stable too rarely (%d/%d) -- merge not exercised' % (stable, total))

    def DEV_ONLY_assert_self_coverage(self):
        # Self-check, NOT a product assertion: confirm the run exercised its surface (the merge,
        # positional chains, as-of-past reads, both non-snapshot isolation levels, verify). A failure
        # here means the TEST stopped covering a dimension, not that the product is wrong.
        self.DEV_ONLY_assert_merge_exercised()
        self.assertGreater(self.state.n_positional, 0, 'no positional update/remove ops were exercised')
        self.assertGreater(self.state.n_read_ts, 0, 'no read_timestamp (as-of-past) txns were exercised')
        self.assertGreater(self.state.n_iso_rc, 0, 'no read-committed txns were exercised')
        self.assertGreater(self.state.n_iso_ru, 0, 'no read-uncommitted txns were exercised')
        self.assertGreater(self.state.n_verify, 0, 'no verify ops were exercised')

    # --- read application + comparison -----------------------------------

    def _anchor(self, lead, foll):
        # A positional write operates on each cursor's CURRENT position, so anchor cur_pos only when
        # leader and follower ended on the SAME key (search_near may land them on different neighbours).
        if lead[0] == 0 and foll[0] == 0 and lead[1] == foll[1]:
            self.state.cur_pos = lead[1]
        else:
            self.state.cur_pos = None

    def _read(self, nodes, trace, do):
        # The per-op oracle: run do(cursor) -> code on the layered then the reference cursor of both
        # nodes, compare, and anchor cur_pos. search_near has its own comparison (_read_near).
        def normalize(cursor):
            ret = do(cursor)
            if ret == wiredtiger.WT_NOTFOUND:
                return (ret, None, None)
            return (0, cursor.get_key(), cursor.get_value())
        layered = []
        for n in nodes:
            ret_dsc = normalize(n.dsc_c)
            ret_asc = normalize(n.asc_c)
            if ret_dsc != ret_asc:
                self.report_mismatch(n, ret_dsc, ret_asc, trace, 'read result differs')
            layered.append(ret_dsc)
        self._anchor(layered[0], layered[1])

    def _near(self, cursor, key):
        cursor.set_key(key)
        cmp = cursor.search_near()
        if cmp == wiredtiger.WT_NOTFOUND:
            return (wiredtiger.WT_NOTFOUND, None, None, None)
        return (0, cursor.get_key(), cursor.get_value(), cmp)

    def _read_near(self, nodes, trace, key):
        # search_near on both nodes: WT may return either immediate neighbour of an absent key.
        layered = []
        for n in nodes:
            ret_dsc = self._near(n.dsc_c, key)
            ret_asc = self._near(n.asc_c, key)
            self._compare_near(n, key, ret_dsc, ret_asc, trace)
            layered.append(ret_dsc)
        self._anchor(layered[0], layered[1])

    # Q: Does it make sense to create a separate class with named fields for ret_dsc/asc.
    def _compare_near(self, node, search_key, ret_dsc, ret_asc, trace):
        (ret_left, key_left, value_left, cmp_left) = ret_dsc
        (ret_right, key_right, value_right, cmp_right) = ret_asc

        # Handle the NOT_FOUND case
        if ret_left == wiredtiger.WT_NOTFOUND or ret_right == wiredtiger.WT_NOTFOUND:
            if ret_left != ret_right:
                self.report_mismatch(node, ret_dsc, ret_asc, trace, 'search_near: one side NOTFOUND')
            return

        # Check the cmp returned from search_near().
        _search_near_cmp = lambda n: (n > 0) - (n < 0)
        if cmp_left != _search_near_cmp(key_left - search_key) or cmp_right != _search_near_cmp(key_right - search_key):
            self.report_mismatch(node, ret_dsc, ret_asc, trace, 'layered search_near cmp sign wrong')
        if key_left == key_right:
            if cmp_left != cmp_right or value_left != value_right:
                self.report_mismatch(node, ret_dsc, ret_asc, trace, 'search_near same key, differing cmp/value')
            return

        # Different immediate neighbours: must bracket the search key and be adjacent. Step the
        # reference cursor by exactly one onto the layered cursor's key to re-sync.
        # Q: Should the results be on the same distance from search_key() in this case?
        lo, hi = sorted((key_left, key_right))
        if not (lo < search_key < hi):
            self.report_mismatch(node, ret_dsc, ret_asc, trace, 'search_near neighbours do not bracket key')
        stepped = node.asc_c.next() if key_right < key_left else node.asc_c.prev()
        if stepped != 0 or node.asc_c.get_key() != key_left or node.asc_c.get_value() != value_left:
            self.report_mismatch(node, ret_dsc, ret_asc, trace, 'search_near neighbours not adjacent / value')

    def report_mismatch(self, node, ret_dsc, ret_asc, trace, reason):
        # The failing op is the last line written to the trace file.
        role = 'leader' if node.conn == self.conn else 'follower'
        self.fail('\n'.join([
            'layered-vs-reference mismatch on %s node: %s' % (role, reason),
            'layered:   %r' % (ret_dsc,),
            'reference: %r' % (ret_asc,),
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
            dsc = self.scan(n.dsc_c, True)
            asc = self.scan(n.asc_c, True)
            if dsc != asc:
                self.fail('full-scan layered != reference (trace %s)\nlayered=%r\nref=%r'
                          % (trace.path, dsc, asc))
            per_node.append(dsc)
        if per_node[0] != per_node[1]:
            self.fail('leader layered scan != follower layered scan (trace %s)' % trace.path)

    # --- operations -------------------------------------------------------
    # Each op is self-contained: it generates its own argument, traces itself, does the work, and
    # updates the model. Shared work lives in helpers (_read / _read_near / _positional /
    # mirror_write / _end_txn / _checkpoint / verify).

    def op_next(self, nodes, rnd, trace):
        trace.log('next')
        self._read(nodes, trace, lambda c: c.next())

    def op_prev(self, nodes, rnd, trace):
        trace.log('prev')
        self._read(nodes, trace, lambda c: c.prev())

    def op_search(self, nodes, rnd, trace):
        key = self.pick_search_key(rnd)
        trace.log('search %r' % key)
        self._read(nodes, trace, lambda c: (c.set_key(key), c.search())[1])

    def op_search_near(self, nodes, rnd, trace):
        key = self.pick_search_key(rnd)
        trace.log('search_near %r' % key)
        self._read_near(nodes, trace, key)

    def op_reset(self, nodes, rnd, trace):
        trace.log('reset')
        for n in nodes:
            n.reset_all()
        self.state.cur_pos = None

    def op_put(self, nodes, rnd, trace):
        key = rnd.choice(self.POOL)
        trace.log('put %r' % key)
        v = self.new_value(key)
        self.mirror_write(nodes, key, v)
        self.state.py_table[key] = v
        self.state.cur_pos = None

    def op_remove(self, nodes, rnd, trace):
        key = rnd.choice(list(self.state.py_table))   # needs_live ensures the table is non-empty
        trace.log('remove %r' % key)
        self.mirror_write(nodes, key, None)
        self.state.py_table.pop(key, None)
        self.state.cur_pos = None

    def op_pos_update(self, nodes, rnd, trace):
        # Positional write: keeps the cursor on cur_pos.
        key = self.state.cur_pos
        trace.log('pos_update %r' % key)
        v = self.new_value(key)
        self._positional(nodes, lambda c: (c.set_value(v), c.update())[1])
        self.state.py_table[key] = v

    def op_pos_remove(self, nodes, rnd, trace):
        # Removes the current key; the cursor stays on the (now deleted) slot.
        key = self.state.cur_pos
        trace.log('pos_remove %r' % key)
        self._positional(nodes, lambda c: c.remove())
        self.state.py_table.pop(key, None)

    def op_begin(self, nodes, rnd, trace):
        # Q: What's that comment about, explain in simpler terms with an example? We fixed WT-17796, is this example still actual?
        # The cursor stays physically positioned so next/prev iterate across the switch, but a
        # positional WRITE must be re-established by a read inside this txn (a write off a pre-txn
        # position is the cross-txn positioned-remove WT-17796), so clear the generator position.
        # Choose the begin flavour from op_begin's own sub-weights (not a top-level pick).
        tw = self.weights.txn_begin
        mode = rnd.choices(
            [Txn.SNAPSHOT, Txn.READ_COMMITTED, Txn.READ_UNCOMMITTED, Txn.READ_TIMESTAMP],
            weights=[tw.snapshot, tw.read_committed, tw.read_uncommitted, tw.read_timestamp],
            k=1)[0]
        # READ_TIMESTAMP needs a past window [oldest, latest]; without one it falls back to snapshot.
        # The oldest_ts>=1 gate keeps randint off timestamp 0 (an invalid read_timestamp).
        read_ts = None
        if mode is Txn.READ_TIMESTAMP:
            # Q: Why do we need to check for self.state.oldest_ts >= 1 and self.state.ts > self.state.oldest_ts here? ts cannot be < than oldest_ts, right? then if ts == oldest_ts we will always scan this last ts avaiable? and even if oldest_ts == 0 - does it mean that we haven't picked up a checkpoint yet? Can we just scan ts = 0 then? Or would it be simpler to assign oldest_ts = 1 by default?
            if self.state.oldest_ts >= 1 and self.state.ts > self.state.oldest_ts:
                read_ts = rnd.randint(self.state.oldest_ts, self.state.ts)
            else:
                mode = Txn.SNAPSHOT
        trace.log('begin %r' % ((read_ts, mode.value),))

        # Build the config
        cfg_parts = []
        if mode in (Txn.READ_COMMITTED, Txn.READ_UNCOMMITTED):
            cfg_parts.append('isolation=' + mode.value)
        if read_ts is not None:
            cfg_parts.append('read_timestamp=' + self.timestamp_str(read_ts))
        for n in nodes:
            n.session.begin_transaction(','.join(cfg_parts))

        # Update state
        self.state.txn = mode
        self.state.txn_wrote = False
        self.state.txn_read_ts = read_ts
        self.state.py_table_snapshot = dict(self.state.py_table)
        self.state.cur_pos = None

        # Collect statistic
        if mode is Txn.READ_TIMESTAMP:
            self.state.n_read_ts += 1
        elif mode is Txn.READ_COMMITTED:
            self.state.n_iso_rc += 1
        elif mode is Txn.READ_UNCOMMITTED:
            self.state.n_iso_ru += 1

    def op_commit(self, nodes, rnd, trace):
        trace.log('commit')
        self._end_txn(nodes, commit=True)

    def op_rollback(self, nodes, rnd, trace):
        trace.log('rollback')
        self._end_txn(nodes, commit=False)

    def _checkpoint(self, nodes):
        # Release any pinned snapshot (reset cursors), advance the checkpoint, clear position.
        for n in nodes:
            n.reset_all()
        self.advance()
        self.state.cur_pos = None

    def op_advance(self, nodes, rnd, trace):
        trace.log('advance')
        self._checkpoint(nodes)

    # Q: is it required to do a checkpoint right before we evict? What if we evict without having everything in the stable table too. My intuition is that it shouldn't let us evict what's not backed by the stable table. Another thing to check (maybe leave a TODO), is that such scenario as evict, might require resetting the cursor since if it's positioned it might be not able to pick up the last checkpoint so it won't release it and so we cannot evict.
    # Q: I thought about it a bit more and we probably should have 2 eviction modes - one is when we don't do the checkpoint and just in random period of time evict everything we can, and the other one is when we do a checkpoint, reset the cursor, and predictably evict 20,40,60,80 or 100% of the ingest table based on random generation, but it could be left for now as TODO.
    def op_evict(self, nodes, rnd, trace):
        # Advance, then drain the follower ingest so later reads fall through to stable.
        trace.log('evict')
        self._checkpoint(nodes)
        self.drain_ingest(nodes[1])

    # Q: verify word is already used for session->verify() command. This should be named scan(), not verify()
    def op_verify(self, nodes, rnd, trace):
        # Full-table scan (the verify() body) as a weighted op: catches a divergence in keys the
        # random reads never touched, and -- scanning the whole follower table through the merged
        # cursor -- exercises the stable constituent for drained keys. Position-breaking.
        trace.log('verify')
        self.verify(nodes, trace)
        self.state.n_verify += 1
        self.state.cur_pos = None

    # --- operation generation -------------------------------------------

    def _legal(self, op, positioned):
        # Which ops are legal in the current transaction context / position -- pure off the Op tags
        # and self.state.txn.
        txn = self.state.txn
        if op.needs_position and not positioned:
            return False
        if op.needs_live and not self.state.py_table:
            return False
        if txn is Txn.NO:
            return not op.in_txn_only            # autocommit: everything except commit/rollback
        if op.autocommit_only:                   # begin/advance/evict not allowed inside a txn
            return False
        if not write_allowed(txn):
            return not op.is_write               # no writes in a read-only transaction
        return True                              # snapshot txn: reads + writes + commit/rollback

    # Q: Can we move all the DEV_ONLY functions to a separate place somewhere in the beginning (so they don't pollute the code while I review it)
    def DEV_ONLY_validate_ops(self):
        # Guard the op table at setup: exactly one row per op_* method (a missing/duplicate row fails
        # loudly here, not deep in a run) and every weight positive (zero/negative would drop an op).
        methods = {n for n in dir(self) if n.startswith('op_')}
        rows = {op.fn.__name__ for op in self.ops}
        assert len(rows) == len(self.ops), 'duplicate op rows in the workload table'
        assert rows == methods, 'op table != op_* methods: %r' % sorted(rows ^ methods)
        assert all(op.weight > 0 for op in self.ops), 'every op weight must be positive'

    # Q: Can we rename pick_op() to run_op() and remove the need to run the selected op on the outer level?
    def pick_op(self, nodes, rnd, trace):
        # Sample one legal op by its weight and return its method bound to context. next/prev
        # dominate the weights, so chains stay long.

        # Q: How current pos may be not None and not in live?
        positioned = self.state.cur_pos is not None and self.state.cur_pos in self.state.py_table
        cands = [op for op in self.ops if self._legal(op, positioned)]
        op = rnd.choices(cands, weights=[op.weight for op in cands], k=1)[0]
        return lambda: op.fn(nodes, rnd, trace)

    # Q: What do the `r` mean here? Why do we do < 0.5 or < 0.8 and what does it mean? Why do we need it?
    def pick_search_key(self, rnd):
        r = rnd.random()
        if self.state.py_table and r < 0.5:
            return rnd.choice(list(self.state.py_table))
        if r < 0.8:
            return rnd.choice(self.POOL)
        return rnd.choice(self.POOL) + 5   # off-grid gap, always absent

    # --- driver ----------------------------------------------------------

    def open_trace(self, seed, tag):
        path = os.path.join(os.getcwd(), 'stress_trace_%s_%d.txt' % (tag, seed))
        self.pr('SEED=%d trace=%s' % (seed, path))
        return EventTrace(path, 'test_layered_cursor_stress seed=%d tag=%s' % (seed, tag))

    def run_sequence(self, seed, tag, n_ops):
        rnd = random.Random(seed)
        trace = self.open_trace(seed, tag)
        nodes = self.make_nodes(tag)
        try:
            for _ in range(n_ops):
                self.pick_op(nodes, rnd, trace)()   # pick_op returns a callable bound to its context
            if self.state.txn is not Txn.NO:
                self._end_txn(nodes, commit=True)   # close any txn left open before verifying
            self.verify(nodes, trace)
        finally:
            # A txn left open by a mid-sequence failure rolls back when the connection closes at
            # teardown (these are never prepared), so no explicit rollback here.
            for n in nodes:
                n.close()
            trace.close()

    # --- tests -----------------------------------------------------------

    def test_smoke(self):
        # Short seeded run with writes, starting from empty tables.
        self.setup_connections(Weights())
        self.run_sequence(seed=12345, tag='smoke', n_ops=80)

    def test_random(self):
        # Mixed read/write/advance/evict sequences, fresh tables per seed, start empty.
        self.setup_connections(Weights())
        for seed in range(10):
            self.run_sequence(seed=seed, tag='r%d' % seed, n_ops=300)
        # Self-check that the run actually exercised the surface (not a product assertion).
        self.DEV_ONLY_assert_self_coverage()
