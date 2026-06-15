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
    # The generator's current transaction context; the value is the isolation= string where one
    # applies. READ_TIMESTAMP is an as-of-past read; write_allowed() says which contexts permit writes.
    NO = 'no'                              # autocommit -- no explicit transaction
    SNAPSHOT = 'snapshot'                  # read-write snapshot transaction
    READ_COMMITTED = 'read-committed'      # read-only
    READ_UNCOMMITTED = 'read-uncommitted'  # read-only
    READ_TIMESTAMP = 'read-timestamp'      # snapshot isolation + an as-of-past read; read-only

def write_allowed(txn):
    return txn in (Txn.NO, Txn.SNAPSHOT)


# --- workload shape --------------------------------------------------------
# Each operation is an Op (built in _build_ops()) pairing an op method -- referenced directly, not
# dispatched by string -- with its weight and legality tags. Adding an op = one op_<name> method +
# one Op row + one Weights field. TxnModeWeights / SearchKeyWeights are sub-distributions an op reads
# directly, not top-level pool weights.

# TODO(workload-tuning): rough first pass (reads dominate; breaks/txn/scenarios rare). Once the long
# run lands, tune to drive the follower stable-read fraction up (see DEV_ONLY_assert_merge_exercised)
# and decide whether a derived break-frequency knob should return.
# FIXME-WT-17827: add a modify op once fixed (modify on a deleted slot aborts the follower layered cursor).
# TODO: add scenario for bulk insert to grow tables significantly.
# FIXME-WT-17825: add prepared transactions once fixed (prepare misbehaves on the follower layered cursor).
# TODO: Should we also always check that the leader and the follower return the same results?

@dataclass(frozen=True)
class TxnModeWeights:
    # op_txn_begin's sub-weights: which flavour to begin (snapshot is read-write; the rest read-only,
    # read_timestamp = an as-of-past read), and how to end an open txn (commit vs rollback).
    snapshot: int = 72
    read_committed: int = 16
    read_uncommitted: int = 12
    read_timestamp: int = 30
    commit: int = 90
    rollback: int = 10

@dataclass(frozen=True)
class SearchKeyWeights:
    # op_search / op_search_near pick an existing key, or a missing one (absent from py_table -- often
    # a removed key, exercising tombstones and the search_near neighbour logic).
    existing: int = 50
    missing: int = 50

@dataclass(frozen=True)
class RemoveKeyWeights:
    # op_remove picks an existing key (a real delete that mutates state) or a missing one (layered and
    # reference must return the same not-found result -- removing an absent/tombstoned key).
    existing: int = 80
    missing: int = 20

@dataclass(frozen=True)
# Q: Question for the future. Some weights should be less than 1% probability (like evict or advance) - probably 0.1% or even 0.01% for the long running tests Should we just make other weights bigger
class Weights:
    # Position-holding ops (reads + positional writes) carry the big weights so chains stay long and
    # the cursor is usually positioned -- the heart of the test.
    next: int = 40
    prev: int = 40
    search: int = 12
    search_near: int = 10
    pos_update: int = 14
    pos_remove: int = 8
    put: int = 6
    remove: int = 2
    reset: int = 2
    full_scan: int = 4
    advance_checkpoint: int = 6
    evict: int = 6
    txn_begin: int = 8
    txn_mode: TxnModeWeights = field(default_factory=TxnModeWeights)
    search_key: SearchKeyWeights = field(default_factory=SearchKeyWeights)
    remove_key: RemoveKeyWeights = field(default_factory=RemoveKeyWeights)

@dataclass(frozen=True)
class Op:
    fn: object                    # the op method, called as fn(nodes, rnd, trace); the dispatch identity
    weight: int                   # relative frequency among the legal ops at each step
    needs_position: bool = False  # cursor must be positioned (positional writes)
    is_write: bool = False        # a logical write (illegal in a read-only transaction)
    no_txn: bool = True           # legal with no open txn (autocommit)
    in_txn: bool = True           # legal inside an open txn

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
        self.oldest_ts = 0       # current oldest_timestamp; floor for legal read_timestamps (C2)
        self.last_advance_checkpoint_ts = 0  # self.ts at the previous advance_checkpoint; oldest lags to here
        self.n_positional = 0    # positional update/remove ops applied (long-lived-chain guard)
        self.n_read_ts = 0       # as-of-past read transactions opened (read_timestamp guard)
        self.n_iso_rc = 0        # read-committed transactions opened (isolation guard, C3)
        self.n_iso_ru = 0        # read-uncommitted transactions opened (isolation guard, C3)
        self.n_full_scan = 0     # full-table cross-checks run (scen_full_scan guard)
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

    disagg_storages = gen_disagg_storages('test_layered_cursor_stress', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    # --- cluster setup ---------------------------------------------------

    def setup_connections(self, weights, n_keys=90):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        self.state = State()
        self.weights = weights
        # Candidate keys spread by 10 so search_near targets fall between them.
        self.pool = list(range(100, 100 + n_keys * 10, 10))
        self.ops = self._build_ops(weights)   # the workload table (Op rows -> op methods)

        self.DEV_ONLY_validate_ops()          # the table must match the op_* methods exactly

        # Advancing to an unchanged checkpoint logs an expected WARNING.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')

    def _build_ops(self, weights):
        # One Op row per op, pairing the op method (the dispatch identity) with its legality tags and
        # the weight copied from the Weights field it belongs to, so run_op samples plain rows.

        # Scenarios are ops that don't logically match to any cursor operation.
        return [
            Op(self.op_next,        weights.next),
            Op(self.op_prev,        weights.prev),
            Op(self.op_search,      weights.search),
            Op(self.op_search_near, weights.search_near),
            Op(self.op_pos_update,  weights.pos_update, needs_position=True, is_write=True),
            Op(self.op_pos_remove,  weights.pos_remove, needs_position=True, is_write=True),
            Op(self.op_put,         weights.put,        is_write=True),
            Op(self.op_remove,      weights.remove,     is_write=True),
            Op(self.op_reset,       weights.reset),
            Op(self.scen_full_scan, weights.full_scan),
            Op(self.scen_advance_checkpoint, weights.advance_checkpoint, in_txn=False),
            Op(self.scen_evict,     weights.evict,      in_txn=False),
            Op(self.op_txn_begin,   weights.txn_begin),
        ]

    # --- DEV_ONLY: setup guard + end-of-run coverage self-checks ---------------------------------
    # Removable scaffolding -- NOT the oracle. Grouped here so they stay out of the core op / read /
    # write logic during review.
    def DEV_ONLY_validate_ops(self):
        # Guard the op table at setup: exactly one row per op_* / scen_* method (a missing/duplicate
        # row fails loudly here, not deep in a run) and every weight positive (0/negative drops an op).
        methods = {n for n in dir(self) if n.startswith(('op_', 'scen_'))}
        rows = {op.fn.__name__ for op in self.ops}
        assert len(rows) == len(self.ops), 'duplicate op rows in the workload table'
        assert rows == methods, 'op table != op_*/scen_* methods: %r' % sorted(rows ^ methods)
        assert all(op.weight > 0 for op in self.ops), 'every op weight must be positive'

    def DEV_ONLY_follower_read_split(self):
        # Follower layered reads served from stable vs ingest. Uses only next/prev/search; search_near's
        # stable counter is impure (counts the current_cursor==NULL case, FIXME-WT-15545).
        stat_cursor = self.session_follow.open_cursor('statistics:')
        try:
            get_stat = lambda name: stat_cursor[getattr(wiredtiger.stat.conn, name)][2]
            stable = get_stat('layered_curs_next_stable') + get_stat('layered_curs_prev_stable') + \
                get_stat('layered_curs_search_stable')
            ingest = get_stat('layered_curs_next_ingest') + get_stat('layered_curs_prev_ingest') + \
                get_stat('layered_curs_search_ingest')
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
        # positional chains, as-of-past reads, both non-snapshot isolation levels, full_scan). A failure
        # here means the TEST stopped covering a dimension, not that the product is wrong.
        self.DEV_ONLY_assert_merge_exercised()
        self.assertGreater(self.state.n_positional, 0, 'no positional update/remove ops were exercised')
        self.assertGreater(self.state.n_read_ts, 0, 'no read_timestamp (as-of-past) txns were exercised')
        self.assertGreater(self.state.n_iso_rc, 0, 'no read-committed txns were exercised')
        self.assertGreater(self.state.n_iso_ru, 0, 'no read-uncommitted txns were exercised')
        self.assertGreater(self.state.n_full_scan, 0, 'no full_scan ops were exercised')

    def make_nodes(self, tag):
        # The layered table must share a name across connections so the follower picks up
        # the leader's checkpoint; the plain reference tables are independent per connection.
        dsc = 'layered:lcs_dsc_%s' % tag
        asc = 'table:lcs_asc_%s' % tag
        cfg = 'key_format=i,value_format=S'
        for session in (self.session, self.session_follow):
            session.create(dsc, cfg)
            session.create(asc, cfg)
        self.state.new_sequence()
        return [Node(self.conn, self.session, dsc, asc),
                Node(self.conn_follow, self.session_follow, dsc, asc)]

    # --- write protocol --------------------------------------------------

    def new_value(self, key):
        self.state.wseq += 1
        return 'v%d.%d' % (key, self.state.wseq)

    def _write_txn(self, nodes, do, label):
        notfound = False
        def step(n):
            nonlocal notfound
            ret_dsc = do(n.dsc_c); ret_asc = do(n.asc_c)
            self.assertEqual(ret_dsc, ret_asc,
                '%s result differs layered=%r reference=%r' % (label, ret_dsc, ret_asc))
            if ret_dsc == wiredtiger.WT_NOTFOUND:
                notfound = True

        if self.state.txn is not Txn.NO:
            for n in nodes:
                step(n)
            self.state.txn_wrote = True
        else:
            self.state.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                step(n)
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.state.ts))
        return notfound

    def _positional(self, nodes, do, label):
        # Positional update/remove off the cursor's held position; clear cur_pos if the write missed.
        if self._write_txn(nodes, do, label):
            self.state.cur_pos = None
        self.state.n_positional += 1

    def commit_txn(self, nodes):
        # A txn that wrote needs a commit_timestamp (layered = ordered write timestamps).
        commit_cfg = ''
        if self.state.txn_wrote:
            self.state.ts += 1
            commit_cfg = 'commit_timestamp=' + self.timestamp_str(self.state.ts)
        for n in nodes:
            n.session.commit_transaction(commit_cfg)

        # FIXME-WT-17830: a follower layered cursor held across an as-of-past txn commit
        # mis-iterates (stays on its key instead of moving). Reset works around it; remove once fixed.
        if self.state.txn_read_ts is not None:
            for n in nodes:
                n.reset_all()
            self.state.cur_pos = None
        self._reset_txn_state()

    def rollback_txn(self, nodes):
        for n in nodes:
            n.session.rollback_transaction()
        self.state.py_table = self.state.py_table_snapshot   # undo the txn's logical writes
        self.state.cur_pos = None                            # rollback resets the session's cursors
        self._reset_txn_state()

    def _reset_txn_state(self):
        self.state.txn = Txn.NO
        self.state.txn_wrote = False
        self.state.txn_read_ts = None
        self.state.py_table_snapshot = None

    def advance_checkpoint(self):
        # Fold the leader's stable into the follower via a new checkpoint.
        if self.state.ts == 0:
            return
        oldest = max(1, self.state.last_advance_checkpoint_ts)
        self.conn.set_timestamp('oldest_timestamp=%s,stable_timestamp=%s'
                                % (self.timestamp_str(oldest), self.timestamp_str(self.state.ts)))
        self.state.oldest_ts = oldest
        self.state.last_advance_checkpoint_ts = self.state.ts
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def force_evict(self, node):
        # Force-evict the follower's ingest leaf so checkpointed keys fall through to stable on later
        # reads: reconciliation drops entries below the prune timestamp (already in stable) and keeps
        # fresher ones. This is the production lifecycle that drives the follower to read from stable.
        ingest_uri = 'file:' + node.dsc_uri[len('layered:'):] + '.wt_ingest'
        evict_cursor = node.session.open_cursor(ingest_uri, None, 'debug=(release_evict)')
        try:
            for key in list(self.state.py_table):
                evict_cursor.set_key(key)
                if evict_cursor.search() == 0:
                    evict_cursor.reset()
        finally:
            evict_cursor.close()

    # --- read application + comparison -----------------------------------

    def _read(self, nodes, trace, do):
        # The per-op oracle: run do(cursor) on the layered then reference cursor of both nodes and
        # compare (ret, key, value), then track cur_pos for a later positional write.
        def op_result(cursor):
            ret = do(cursor)
            if ret == wiredtiger.WT_NOTFOUND:
                return (ret, None, None)
            return (0, cursor.get_key(), cursor.get_value())

        for n in nodes:
            ret_dsc = op_result(n.dsc_c)
            ret_asc = op_result(n.asc_c)
            if ret_dsc != ret_asc:
                self.report_mismatch(n, ret_dsc, ret_asc, trace, 'read result differs')
            ret, key, _ = ret_dsc
            self.state.cur_pos = key if ret == 0 else None

    def _search_near_ceiling(self, cursor, key):
        # Canonicalize search_near to the CEILING (smallest key >= key). WT may return either
        # neighbour of an absent key; if it lands below key, step one to the next. Deterministic, so
        # leader/follower (and asc/dsc) land on the same key and search_near compares like any read.
        cursor.set_key(key)
        cmp = cursor.search_near()
        if cmp == wiredtiger.WT_NOTFOUND:
            return wiredtiger.WT_NOTFOUND
        return cursor.next() if cmp < 0 else 0

    def report_mismatch(self, node, ret_dsc, ret_asc, trace, reason):
        # The failing op is the last line written to the trace file.
        role = 'leader' if node.conn == self.conn else 'follower'
        self.fail('\n'.join([
            'layered-vs-reference mismatch on %s node: %s' % (role, reason),
            'layered:   %r' % (ret_dsc,),
            'reference: %r' % (ret_asc,),
            'trace file: %s' % trace.path]))

    def pick_key(self, rnd, w):
        # Existing key vs missing key, weighted by the given existing/missing config.
        if self.state.py_table and rnd.choices((True, False), weights=(w.existing, w.missing))[0]:
            return rnd.choice(list(self.state.py_table))

        # pool step is 10, so if every pool key is live, pool_key + 5 is guaranteed absent.
        absent = [k for k in self.pool if k not in self.state.py_table]
        return rnd.choice(absent) if absent else rnd.choice(self.pool) + 5

    # --- verification ----------------------------------------------------

    def _scan_cursor(self, cursor, forward):
        cursor.reset()
        out = []
        while True:
            ret = cursor.next() if forward else cursor.prev()
            if ret == wiredtiger.WT_NOTFOUND:
                break
            out.append((cursor.get_key(), cursor.get_value()))
        return out

    def full_scan(self, nodes, trace):
        # Whole-table cross-check.
        per_node = []
        for n in nodes:
            dsc = self._scan_cursor(n.dsc_c, True)
            asc = self._scan_cursor(n.asc_c, True)
            if dsc != asc:
                self.fail('full-scan layered != reference (trace %s)\nlayered=%r\nref=%r'
                          % (trace.path, dsc, asc))
            per_node.append(dsc)
        if per_node[0] != per_node[1]:
            self.fail('leader layered scan != follower layered scan (trace %s)' % trace.path)

    # --- operations -------------------------------------------------------
    # Each op is self-contained: it generates its own argument, traces itself, does the work, and
    # updates the model.

    def op_next(self, nodes, rnd, trace):
        trace.log('next')
        self._read(nodes, trace, lambda c: c.next())

    def op_prev(self, nodes, rnd, trace):
        trace.log('prev')
        self._read(nodes, trace, lambda c: c.prev())

    def op_search(self, nodes, rnd, trace):
        key = self.pick_key(rnd, self.weights.search_key)
        trace.log('search %r' % key)
        self._read(nodes, trace, lambda c: (c.set_key(key), c.search())[1])

    def op_search_near(self, nodes, rnd, trace):
        key = self.pick_key(rnd, self.weights.search_key)
        trace.log('search_near %r' % key)
        self._read(nodes, trace, lambda c: self._search_near_ceiling(c, key))

    def op_reset(self, nodes, rnd, trace):
        trace.log('reset')
        for n in nodes:
            n.reset_all()
        self.state.cur_pos = None

    def op_put(self, nodes, rnd, trace):
        key = rnd.choice(self.pool)
        trace.log('put %r' % key)
        value = self.new_value(key)
        self._write_txn(nodes, lambda c: (c.set_key(key), c.set_value(value), c.insert())[-1], 'put')
        self.state.py_table[key] = value
        self.state.cur_pos = None

    def op_remove(self, nodes, rnd, trace):
        # An existing key (a real delete) or a missing one (layered and reference must agree).
        key = self.pick_key(rnd, self.weights.remove_key)
        trace.log('remove %r' % key)
        self._write_txn(nodes, lambda c: (c.set_key(key), c.remove())[-1], 'remove')
        self.state.py_table.pop(key, None)
        self.state.cur_pos = None

    def op_pos_update(self, nodes, rnd, trace):
        # Positional write: keeps the cursor on cur_pos.
        key = self.state.cur_pos
        trace.log('pos_update %r' % key)
        value = self.new_value(key)
        self._positional(nodes, lambda c: (c.set_value(value), c.update())[-1], 'pos_update')
        self.state.py_table[key] = value

    def op_pos_remove(self, nodes, rnd, trace):
        # Removes the current key; the cursor stays on the (now deleted) slot.
        key = self.state.cur_pos
        trace.log('pos_remove %r' % key)
        self._positional(nodes, lambda c: c.remove(), 'pos_remove')
        self.state.py_table.pop(key, None)

    def op_txn_begin(self, nodes, rnd, trace):
        # No txn open -> begin one (flavour by the txn_mode weights); a txn open -> end it.
        txn_weights = self.weights.txn_mode

        # Close a txn is one is running
        if self.state.txn is not Txn.NO:
            if rnd.choices((True, False), weights=(txn_weights.commit, txn_weights.rollback))[0]:
                trace.log('commit')
                self.commit_txn(nodes)
            else:
                trace.log('rollback')
                self.rollback_txn(nodes)
            return

        mode = rnd.choices(
            [Txn.SNAPSHOT, Txn.READ_COMMITTED, Txn.READ_UNCOMMITTED, Txn.READ_TIMESTAMP],
            weights=[txn_weights.snapshot, txn_weights.read_committed, txn_weights.read_uncommitted, txn_weights.read_timestamp],
            k=1)[0]

        # A read_timestamp just needs to be valid: in [oldest_ts, ts] with oldest_ts >= 1.
        read_ts = None
        if mode is Txn.READ_TIMESTAMP:
            if self.state.ts >= self.state.oldest_ts >= 1:
                read_ts = rnd.randint(self.state.oldest_ts, self.state.ts)
            else:
                mode = Txn.SNAPSHOT
        trace.log('txn_begin %r' % ((read_ts, mode.value),))

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

        # Collect statistic
        if mode is Txn.READ_TIMESTAMP:
            self.state.n_read_ts += 1
        elif mode is Txn.READ_COMMITTED:
            self.state.n_iso_rc += 1
        elif mode is Txn.READ_UNCOMMITTED:
            self.state.n_iso_ru += 1

    def _checkpoint(self, nodes):
        # Release any pinned snapshot (reset cursors), advance the checkpoint, clear position.
        for n in nodes:
            n.reset_all()
        self.advance_checkpoint()
        self.state.cur_pos = None

    def scen_advance_checkpoint(self, nodes, rnd, trace):
        trace.log('advance_checkpoint')
        self._checkpoint(nodes)

    def scen_evict(self, nodes, rnd, trace):
        # Checkpoint (resets cursors, releasing pins) THEN drain the follower ingest so later reads
        # fall through to stable. The checkpoint is required to drain everything -- eviction only
        # prunes ingest entries already in stable, so without it the drain might be restricted to evict
        # a big part of the content; and a cursor pinning the ingest leaf blocks eviction, so the reset
        # (in _checkpoint) comes first.

        # TODO(eviction-modes): add two modes -- (a) no-checkpoint, opportunistically evict whatever is
        # already prunable (exercises trying to evict not-yet-stable ingest content), and (b) checkpoint
        # + reset + drain a random 20/40/60/80/100% of ingest for finer control of the ingest/stable
        # split. Today this is mode (b) at 100%.
        trace.log('evict')
        self._checkpoint(nodes)
        follower = nodes[1]
        self.force_evict(follower)

    def scen_full_scan(self, nodes, rnd, trace):
        # full scan of the table, position-breaking.
        trace.log('full_scan')
        self.full_scan(nodes, trace)
        self.state.n_full_scan += 1
        self.state.cur_pos = None

    # --- operation generation -------------------------------------------

    def _legal(self, op):
        # Which ops are legal in the current transaction context / position.

        txn = self.state.txn
        if op.needs_position and self.state.cur_pos is None:
            return False
        if txn is Txn.NO:
            return op.no_txn
        if not op.in_txn:          # advance_checkpoint / evict not allowed inside a txn
            return False
        if not write_allowed(txn):
            return not op.is_write # no writes in a read-only transaction
        return True

    def run_op(self, nodes, rnd, trace):
        # Pick one legal op by its weight and run it. Each op_* method does its own arg generation,
        # tracing, and state update.
        cands = [op for op in self.ops if self._legal(op)]
        op = rnd.choices(cands, weights=[op.weight for op in cands], k=1)[0]
        op.fn(nodes, rnd, trace)

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
                self.run_op(nodes, rnd, trace)
            if self.state.txn is not Txn.NO:
                self.commit_txn(nodes) # close any txn left open before verifying
            # scan the tables as a final verification
            self.full_scan(nodes, trace)
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
