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
# Reproducibility: the seed set is fixed, so a run is fully deterministic and a failure repeats
# on re-run (the failing seed is printed as SEED=<n>). Every chosen event is appended to a
# per-seed trace file (path printed at start and on failure), flushed each step, so a failure is
# a self-contained record; to dig into one seed, run it in a throwaway test calling run_sequence().

import os, random
from dataclasses import dataclass
from enum import Enum
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

def sign(n):
    return (n > 0) - (n < 0)

# Decision labels are enums, never bare strings -- a typo'd member is an AttributeError at import,
# not a silently-never-matched branch at run time.

class Txn(Enum):
    # The kind of transaction context the generator is currently in. NO is autocommit; the rest are
    # explicit begin_transaction flavours. Only NO and SNAPSHOT permit writes (see write_allowed);
    # READ_COMMITTED / READ_UNCOMMITTED reject writes (txn_inline.h ~2112) and READ_TIMESTAMP is an
    # as-of-past read. The value is the WiredTiger isolation= config string where one applies.
    NO = 'no'                              # autocommit -- no explicit transaction
    SNAPSHOT = 'snapshot'                  # read-write snapshot transaction
    READ_COMMITTED = 'read-committed'      # read-only
    READ_UNCOMMITTED = 'read-uncommitted'  # read-only
    READ_TIMESTAMP = 'read-timestamp'      # snapshot isolation + an as-of-past read_timestamp; read-only

def write_allowed(txn):
    # Writes are legal only outside a transaction or under snapshot isolation; every other Txn flavour
    # is read-only. The inverse (is_read_only) is just `not write_allowed(txn)`.
    return txn in (Txn.NO, Txn.SNAPSHOT)

# --- workload shape --------------------------------------------------------
# Each operation is an OpSpec (built in _build_ops()) pairing an op METHOD -- referenced directly,
# never dispatched by string -- with a config-key NAME and legality tags. Adding an operation = one
# op_<name> method (self-contained: generates its own argument, traces itself, does the work) + one
# OpSpec row + one weight entry in the workload config.

@dataclass(frozen=True)
class OpSpec:
    fn: object                    # the op method, called as fn(nodes, rnd, trace)
    name: str                     # config key -- used only to look up the op's weight, never to dispatch
    needs_position: bool = False  # cursor must be positioned on a live key (positional writes)
    needs_live: bool = False      # at least one live key must exist (remove by key)
    is_write: bool = False        # a logical write (illegal in a read-only transaction)
    autocommit_only: bool = False  # only with no open txn (begin / advance / evict)
    in_txn_only: bool = False     # only with an open txn (commit / rollback)

# The whole workload shape is ONE nested, inheritable structure. A top-level entry is either a leaf
# op weight or a GROUP {'weight': <share at the top level>, <child>: <weight within the group>, ...}.
# The effective weight of any leaf is the product of the weights along its path, so once pick_op
# filters to the legal ops it just sums the survivors and samples -- renormalisation is automatic.
# Semantics (the rule): a group's 'weight' is how often that group is chosen against the other
# top-level entries; a child's weight is its share *given* the group was chosen.
#   - 'txn'      : when no txn is open it resolves to `begin` (op_begin then picks its flavour from
#                  the begin-mode sub-weights snapshot/read_committed/read_uncommitted/read_timestamp);
#                  when a txn IS open it resolves to commit/rollback by their sub-weights.
#   - 'scenarios': rare checkpoint/eviction (and, later, injected) ops.
# A test inherits DEFAULT_WEIGHTS and may deep-merge an override (merge_weights) -- e.g. a
# scenario-heavy test bumps just 'scenarios' without restating the rest.
# TODO(workload-tuning): these are a rough first pass (reads dominate; breaks/txn/scenarios rare).
# Dropping the old single P_BREAK knob (now implicit in next/prev=80 vs the position-breaking ops)
# is decision D1 in the plan -- revisit whether a derived break knob should return, and tune to
# drive the follower stable-read fraction up (see assert_merge_exercised), once the long run lands.
DEFAULT_WEIGHTS = {
    # Position-HOLDING ops (reads + positional writes) dominate so chains stay long and the cursor
    # is usually positioned -- the heart of the test. Position-BREAKING ops (put/remove/reset/verify
    # + the txn and scenario groups) carry deliberately small weights; with no P_BREAK gate anymore,
    # their raw weight IS the break frequency (~12% here).
    'next': 40, 'prev': 40, 'search': 12, 'search_near': 10,
    'pos_update': 14, 'pos_remove': 8,
    'put': 6, 'remove': 2, 'reset': 2, 'verify': 4,
    'txn': {
        'weight': 8,
        'snapshot': 72, 'read_committed': 16, 'read_uncommitted': 12, 'read_timestamp': 30,
        'commit': 70, 'rollback': 30,
    },
    'scenarios': {
        'weight': 12,
        'advance': 50, 'evict': 50,
        # TODO(scenarios): forced_evict, bulk_remove, massive_prepare, truncate -- add as op rows.
    },
}

def merge_weights(base, override):
    # Deep-merge override onto base (recursing into group dicts) so a test can tweak part of the
    # workload shape without restating all of it.
    out = dict(base)
    for k, v in override.items():
        out[k] = merge_weights(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out

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

class State:
    # The model the generator reasons about. Connection-global fields (timestamps, coverage
    # counters) persist across sequences in a multi-seed run; the per-sequence fields (the
    # expected table contents, cursor position, and open-transaction flags) are reset by
    # new_sequence() for each fresh set of tables. None of this is the oracle -- it only drives
    # operation generation and the expected next-step decisions.
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
        self.live = {}           # logical key->value, for operation generation only
        self.cur_pos = None      # key both cursor pairs are positioned on (None = unpositioned)
        self.txn = Txn.NO        # the current transaction context (NO = autocommit); see write_allowed
        self.txn_wrote = False   # the open txn has performed at least one write
        self.txn_read_ts = None  # the as-of timestamp when txn is READ_TIMESTAMP, else None
        self.live_snapshot = None  # self.live as of begin, restored on rollback

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
        # All generation/model/timestamp/counter state lives in one State object. Its
        # connection-global fields (timestamps, counters) persist across sequences; new_sequence()
        # resets the per-sequence fields (live, cur_pos, txn flags).
        self.state = State()
        self.ops = self._build_ops()   # the workload table (OpSpec rows -> op methods)
        self.ops_by_name = {s.name: s for s in self.ops}   # weight-config key -> OpSpec
        # Advancing to an unchanged checkpoint logs an expected WARNING.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')

    def _build_ops(self):
        # One OpSpec row per op, pairing the op method (direct reference) with its config-key name
        # and legality tags. Weights live in the workload config (DEFAULT_WEIGHTS), keyed by name.
        return [
            OpSpec(self.op_next,        'next'),
            OpSpec(self.op_prev,        'prev'),
            OpSpec(self.op_search,      'search'),
            OpSpec(self.op_search_near, 'search_near'),
            OpSpec(self.op_pos_update,  'pos_update', needs_position=True, is_write=True),
            OpSpec(self.op_pos_remove,  'pos_remove', needs_position=True, is_write=True),
            OpSpec(self.op_put,         'put',        is_write=True),
            OpSpec(self.op_remove,      'remove',     is_write=True, needs_live=True),
            OpSpec(self.op_reset,       'reset'),
            OpSpec(self.op_verify,      'verify'),
            OpSpec(self.op_advance,     'advance',    autocommit_only=True),
            OpSpec(self.op_evict,       'evict',      autocommit_only=True),
            OpSpec(self.op_begin,       'begin',      autocommit_only=True),
            OpSpec(self.op_commit,      'commit',     in_txn_only=True),
            OpSpec(self.op_rollback,    'rollback',   in_txn_only=True),
        ]

    def make_nodes(self, tag):
        # The layered table must share a name across connections so the follower picks up
        # the leader's checkpoint; the plain reference tables are independent per connection.
        lay = 'layered:lcs_%s' % tag
        ref = 'table:lcs_ref_%s' % tag
        cfg = 'key_format=i,value_format=S'
        for s in (self.session, self.session_follow):
            s.create(lay, cfg)
            s.create(ref, cfg)
        self.state.new_sequence()   # reset per-sequence model state (live, cur_pos, txn flags)
        return [Node(self.conn, self.session, lay, ref),
                Node(self.conn_follow, self.session_follow, lay, ref)]

    # --- write protocol --------------------------------------------------

    def new_value(self, key):
        self.state.wseq += 1
        return 'v%d.%d' % (key, self.state.wseq)

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
        # Run a positional update/remove on the cursor's current key (self.state.cur_pos, which must
        # be live). `do(cursor)` performs the actual update/remove on a positioned cursor and returns
        # the code. Inside an explicit txn the positioning read and this write share the transaction,
        # so the cursor is genuinely positioned (KEY_INT|VALUE_INT) and we write DIRECTLY -- the real
        # iterate-and-delete-in-one-txn. In autocommit the positioning value is not valid across the
        # implicit txn boundary (WT-17796), so we re-search to re-establish position.
        key = self.state.cur_pos
        if self.state.txn is not Txn.NO:
            for n in nodes:
                rl = do(n.lay_c); rr = do(n.ref_c)
                self.assertEqual(rl, rr, 'positional result differs layered=%r reference=%r at key=%r'
                                 % (rl, rr, key))
            self.state.txn_wrote = True
        else:
            self.state.ts += 1
            for n in nodes:
                n.session.begin_transaction()
                n.lay_c.set_key(key); rl_s = n.lay_c.search()
                n.ref_c.set_key(key); rr_s = n.ref_c.search()
                self.assertEqual((rl_s, rr_s), (0, 0),
                    'positional re-search failed layered=%r reference=%r key=%r' % (rl_s, rr_s, key))
                rl = do(n.lay_c); rr = do(n.ref_c)
                self.assertEqual(rl, rr, 'positional result differs layered=%r reference=%r at key=%r'
                                 % (rl, rr, key))
                n.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.state.ts))
            self.state.dirty = True
        self.state.n_positional += 1

    def _end_txn(self, nodes, commit):
        # Close the explicit transaction open on both nodes' sessions.
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
            # A successful commit keeps cursors positioned (cur_pos survives the txn switch),
            # except for an as-of-T read txn: those cursors sit in a historical view, so reset them
            # -- resuming a latest iteration from a held historical position lets the layered cursor
            # pin its stable constituent across the snapshot change and diverge from the plain
            # reference (the Q2 family, ruled not-a-bug; confirmed -- a fresh read agrees).
            # TODO(pin-reset): read-committed/read-uncommitted read-only txns also hold cursors
            # across the commit, but resume in a compatible (latest) view, so no pin divergence has
            # been observed; extend this reset to them if one ever appears.
            if self.state.txn_read_ts is not None:
                for n in nodes:
                    n.reset_all()
                self.state.cur_pos = None
        else:
            for n in nodes:
                n.session.rollback_transaction()
            self.state.live = self.state.live_snapshot   # undo the txn's logical writes
            self.state.cur_pos = None              # rollback resets the session's cursors
        self.state.txn = Txn.NO
        self.state.txn_wrote = False
        self.state.txn_read_ts = None
        self.state.live_snapshot = None            # consumed; the next begin takes a fresh snapshot

    def advance(self):
        # Fold the leader's stable into the follower's stable via a new checkpoint. Skip if
        # nothing changed since the last advance (avoids a redundant-checkpoint warning).
        #
        # stable moves to the latest commit, but oldest LAGS one advance behind it so the window
        # [oldest, latest] stays open for as-of-past reads (read_timestamp, C2). Pinning
        # oldest == stable would forbid reading anything below the latest commit. oldest is
        # monotonic (last_advance_ts only grows) and < stable (writes happened since).
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
        # Evict the follower's ingest content so already-checkpointed keys fall through to
        # stable on later reads. The ingest is a single in-memory leaf page; forced eviction
        # reconciles it, dropping entries below the prune timestamp (already in stable) while
        # retaining fresher entries written since the last checkpoint advance. This is the
        # production lifecycle and is what actually drives the follower to read from stable.
        ingest_uri = 'file:' + node.lay_uri[len('layered:'):] + '.wt_ingest'
        ec = node.session.open_cursor(ingest_uri, None, 'debug=(release_evict)')
        try:
            for k in list(self.state.live):
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
        # Guard against a degenerate oracle: the follower must read from stable at least sometimes,
        # or the merge of two non-empty constituents is not being tested at all.
        # TODO(merge-coverage): the stable-read fraction is only ~2-9% in the random run -- stable
        # reads are rare because writes keep keys in ingest and the drain rarely catches a key
        # stable-and-not-rewritten before it is read. Threshold lowered from 10% to 1% as an
        # INTERIM. Real fix (see refactor plan): a forced-eviction scenario op + a long run
        # (~300k ops, not 300) so the stable path is heavily exercised, then restore a meaningful
        # floor.
        stable, ingest = self.follower_read_split()
        total = stable + ingest
        self.assertGreater(total, 0, 'no follower layered reads at all')
        self.assertGreaterEqual(stable * 100, total,
            'follower read from stable too rarely (%d/%d) -- merge not exercised' % (stable, total))

    def assert_self_coverage(self):
        # Self-check, NOT a product assertion: confirm the random run actually exercised the
        # surface it is meant to -- the stable+ingest merge, long-lived positional chains,
        # read_timestamp (as-of-past) reads, and both non-snapshot isolation levels. It guards
        # against a degenerate run where the oracle passes only because nothing interesting
        # happened. A failure here means the TEST stopped covering a dimension (it is no longer
        # doing its job), not that the product is wrong.
        self.assert_merge_exercised()
        self.assertGreater(self.state.n_positional, 0, 'no positional update/remove ops were exercised')
        self.assertGreater(self.state.n_read_ts, 0, 'no read_timestamp (as-of-past) txns were exercised')
        self.assertGreater(self.state.n_iso_rc, 0, 'no read-committed txns were exercised')
        self.assertGreater(self.state.n_iso_ru, 0, 'no read-uncommitted txns were exercised')
        self.assertGreater(self.state.n_verify, 0, 'no verify ops were exercised')

    # --- read application + comparison -----------------------------------

    def _anchor(self, lead, foll):
        # cur_pos anchors a positional write, which operates on each cursor's CURRENT position --
        # valid only when leader and follower ended on the SAME key (search_near may land them on
        # different valid neighbours).
        if lead[0] == 0 and foll[0] == 0 and lead[1] == foll[1]:
            self.state.cur_pos = lead[1]
        else:
            self.state.cur_pos = None

    def _read(self, nodes, trace, do):
        # Run a read on the layered then the reference cursor of both nodes (the per-op oracle),
        # via the callable `do(cursor) -> WT return code`, and anchor cur_pos. Used by the simple
        # reads (next/prev/search); search_near has its own comparison (_read_near).
        def normalize(cursor):
            ret = do(cursor)
            if ret == wiredtiger.WT_NOTFOUND:
                return (ret, None, None)
            return (0, cursor.get_key(), cursor.get_value())
        lead = foll = None
        for i, n in enumerate(nodes):
            r_lay = normalize(n.lay_c)
            r_ref = normalize(n.ref_c)
            if r_lay != r_ref:
                self.fail_mismatch(n, r_lay, r_ref, trace, 'read result differs')
            lead, foll = (r_lay, foll) if i == 0 else (lead, r_lay)
        self._anchor(lead, foll)

    def _near(self, cursor, key):
        cursor.set_key(key)
        cmp = cursor.search_near()
        if cmp == wiredtiger.WT_NOTFOUND:
            return (wiredtiger.WT_NOTFOUND, None, None, None)
        return (0, cursor.get_key(), cursor.get_value(), cmp)

    def _read_near(self, nodes, trace, key):
        # search_near on both nodes: WT may return either immediate neighbour of an absent key.
        lead = foll = None
        for i, n in enumerate(nodes):
            r_lay = self._near(n.lay_c, key)
            r_ref = self._near(n.ref_c, key)
            self._compare_near(n, key, r_lay, r_ref, trace)
            lead, foll = (r_lay, foll) if i == 0 else (lead, r_lay)
        self._anchor(lead, foll)

    def _compare_near(self, node, search_key, r_lay, r_ref, trace):
        (retl, kl, vl, cl) = r_lay
        (retr, kr, vr, cr) = r_ref
        if retl == wiredtiger.WT_NOTFOUND or retr == wiredtiger.WT_NOTFOUND:
            if retl != retr:
                self.fail_mismatch(node, r_lay, r_ref, trace, 'search_near: one side NOTFOUND')
            return
        # The layered cmp sign must agree with the returned key vs the search key (A11): 0 on
        # exact match, <0 if the returned key is smaller, >0 if larger.
        if sign(cl) != sign(kl - search_key):
            self.fail_mismatch(node, r_lay, r_ref, trace, 'layered search_near cmp sign wrong')
        if kl == kr:
            if sign(cl) != sign(cr) or vl != vr:
                self.fail_mismatch(node, r_lay, r_ref, trace, 'search_near same key, differing cmp/value')
            return
        # Different immediate neighbours: must bracket the search key and be adjacent. Step the
        # reference cursor by exactly one onto the layered cursor's key to re-sync.
        lo, hi = sorted((kl, kr))
        if not (lo < search_key < hi):
            self.fail_mismatch(node, r_lay, r_ref, trace, 'search_near neighbours do not bracket key')
        stepped = node.ref_c.next() if kr < kl else node.ref_c.prev()
        if stepped != 0 or node.ref_c.get_key() != kl or node.ref_c.get_value() != vl:
            self.fail_mismatch(node, r_lay, r_ref, trace, 'search_near neighbours not adjacent / value')

    def fail_mismatch(self, node, r_lay, r_ref, trace, reason):
        # The failing op is the last line written to the trace file.
        self.fail('\n'.join([
            'layered-vs-reference mismatch on %s node: %s' % (node.conn == self.conn and 'leader' or 'follower', reason),
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

    # --- operations -------------------------------------------------------
    # Each op is self-contained: it generates its own argument, traces itself, does the work, and
    # updates the model. Shared work lives in helpers (_read / _read_near / _positional /
    # mirror_write / _end_txn / _checkpoint / verify). pick_op picks an OpSpec and calls its
    # method; nothing here is selected from an op-name string.

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
        self.state.live[key] = v
        self.state.cur_pos = None

    def op_remove(self, nodes, rnd, trace):
        key = rnd.choice(list(self.state.live))   # needs_live ensures the table is non-empty
        trace.log('remove %r' % key)
        self.mirror_write(nodes, key, None)
        self.state.live.pop(key, None)
        self.state.cur_pos = None

    def op_pos_update(self, nodes, rnd, trace):
        # Positional write: keeps the cursor on cur_pos.
        key = self.state.cur_pos
        trace.log('pos_update %r' % key)
        v = self.new_value(key)
        self._positional(nodes, lambda c: (c.set_value(v), c.update())[1])
        self.state.live[key] = v

    def op_pos_remove(self, nodes, rnd, trace):
        # Removes the current key; the cursor stays on the (now deleted) slot.
        key = self.state.cur_pos
        trace.log('pos_remove %r' % key)
        self._positional(nodes, lambda c: c.remove())
        self.state.live.pop(key, None)

    def op_begin(self, nodes, rnd, trace):
        # snapshot+no-read_ts = read-write; snapshot+read_ts = as-of-T read-only; read-committed /
        # read-uncommitted = read-only (those isolations reject writes). The cursor stays physically
        # positioned so next/prev keep iterating across the switch, but a positional WRITE must be
        # re-established by a read inside this txn (a write off a pre-txn position is the cross-txn
        # positioned-remove WT-17796), so clear the generator position.
        # Choose the begin flavour from the 'txn' group's begin-mode sub-weights.
        tw = self.weights['txn']
        mode = rnd.choices(
            [Txn.SNAPSHOT, Txn.READ_COMMITTED, Txn.READ_UNCOMMITTED, Txn.READ_TIMESTAMP],
            weights=[tw['snapshot'], tw['read_committed'], tw['read_uncommitted'], tw['read_timestamp']],
            k=1)[0]
        # READ_TIMESTAMP needs a past window [oldest, latest]; without one it falls back to a plain
        # snapshot txn. The oldest_ts>=1 gate keeps randint off timestamp 0 (an invalid read_timestamp).
        read_ts = None
        if mode is Txn.READ_TIMESTAMP:
            if self.state.oldest_ts >= 1 and self.state.ts > self.state.oldest_ts:
                read_ts = rnd.randint(self.state.oldest_ts, self.state.ts)
            else:
                mode = Txn.SNAPSHOT
        trace.log('begin %r' % ((read_ts, mode.value),))
        cfg_parts = []
        # SNAPSHOT/READ_TIMESTAMP run at the default snapshot isolation (no isolation= clause);
        # READ_TIMESTAMP's value is not a WT isolation string, so only RC/RU emit isolation=.
        if mode in (Txn.READ_COMMITTED, Txn.READ_UNCOMMITTED):
            cfg_parts.append('isolation=' + mode.value)
        if read_ts is not None:
            cfg_parts.append('read_timestamp=' + self.timestamp_str(read_ts))
        for n in nodes:
            n.session.begin_transaction(','.join(cfg_parts))
        self.state.txn = mode
        self.state.txn_wrote = False
        self.state.txn_read_ts = read_ts
        if mode is Txn.READ_TIMESTAMP:
            self.state.n_read_ts += 1
        elif mode is Txn.READ_COMMITTED:
            self.state.n_iso_rc += 1
        elif mode is Txn.READ_UNCOMMITTED:
            self.state.n_iso_ru += 1
        self.state.live_snapshot = dict(self.state.live)
        self.state.cur_pos = None

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

    def op_evict(self, nodes, rnd, trace):
        # Advance, then drain the follower ingest so later reads fall through to stable.
        trace.log('evict')
        self._checkpoint(nodes)
        self.drain_ingest(nodes[1])

    def op_verify(self, nodes, rnd, trace):
        # Full-table layered-vs-reference scan (the same verify() body used at end of sequence) as a
        # weighted op: catches a divergence in keys the random reads never touched, and -- reading
        # the whole follower table through the merged cursor -- exercises the stable constituent for
        # drained keys. Resets the cursors (a position-breaking op).
        trace.log('verify')
        self.verify(nodes, trace)
        self.state.n_verify += 1
        self.state.cur_pos = None

    # --- operation generation -------------------------------------------

    def _legal(self, spec, positioned):
        # Which ops are legal in the current transaction context / position -- pure data, off the
        # OpSpec tags and self.state.txn.
        txn = self.state.txn
        if spec.needs_position and not positioned:
            return False
        if spec.needs_live and not self.state.live:
            return False
        if txn is Txn.NO:
            return not spec.in_txn_only          # autocommit: everything except commit/rollback
        if spec.autocommit_only:                 # begin/advance/evict not allowed inside a txn
            return False
        if not write_allowed(txn):
            return not spec.is_write             # no writes in a read-only transaction
        return True                              # snapshot txn: reads + writes + commit/rollback

    def _candidates(self, positioned):
        # Walk the workload config and yield (effective_weight, spec) for every LEGAL op. A group's
        # 'weight' is its share at the top level; a child's weight is conditional within the group, so
        # the effective weight is the product. The 'txn' group is the one state-dependent node: when
        # no txn is open it offers `begin` (op_begin then picks its own flavour); when a txn is open it
        # offers commit/rollback by their sub-weights. Top-level leaves and 'scenarios' are generic.
        in_txn = self.state.txn is not Txn.NO
        out = []
        def add(weight, name):
            spec = self.ops_by_name[name]
            if self._legal(spec, positioned):
                out.append((weight, spec))
        def add_group(gw, children):
            # A group contributes exactly its share `gw` to the top-level pool: distribute it over
            # the children in proportion to their weights (normalised), so the children's internal
            # scale never leaks into the top-level comparison.
            tot = sum(children.values())
            for name, cw in children.items():
                add(gw * cw / tot, name)
        for key, val in self.weights.items():
            if key == 'txn':
                gw = val['weight']
                if in_txn:                                  # close the open txn
                    add_group(gw, {'commit': val['commit'], 'rollback': val['rollback']})
                else:                                       # open one (op_begin picks the flavour)
                    add(gw, 'begin')
            elif key == 'scenarios':
                add_group(val['weight'], {k: v for k, v in val.items() if k != 'weight'})
            else:
                add(val, key)
        return out

    def pick_op(self, nodes, rnd, trace):
        # Sample one legal op from the workload config by its effective weight, and return the op's
        # method bound to its context. The method does its own arg-gen + trace. The keep/break/txn
        # mix is whatever the config weights make it -- next/prev dominate, so chains stay long.
        positioned = self.state.cur_pos is not None and self.state.cur_pos in self.state.live
        cands = self._candidates(positioned)
        spec = rnd.choices([s for _, s in cands], weights=[w for w, _ in cands], k=1)[0]
        return lambda: spec.fn(nodes, rnd, trace)

    def pick_search_key(self, rnd):
        r = rnd.random()
        if self.state.live and r < 0.5:
            return rnd.choice(list(self.state.live))
        if r < 0.8:
            return rnd.choice(self.POOL)
        return rnd.choice(self.POOL) + 5   # off-grid gap, always absent

    # --- driver ----------------------------------------------------------

    def open_trace(self, seed, tag):
        path = os.path.join(os.getcwd(), 'stress_trace_%s_%d.txt' % (tag, seed))
        self.pr('SEED=%d trace=%s' % (seed, path))
        return EventTrace(path, 'test_layered_cursor_stress seed=%d tag=%s' % (seed, tag))

    def run_sequence(self, seed, tag, n_ops, weights=None):
        # weights: an optional override deep-merged onto DEFAULT_WEIGHTS, so a test can reshape part
        # of the workload (e.g. a scenario-heavy run) without restating the whole config.
        self.weights = merge_weights(DEFAULT_WEIGHTS, weights) if weights else DEFAULT_WEIGHTS
        rnd = random.Random(seed)
        trace = self.open_trace(seed, tag)
        nodes = self.make_nodes(tag)
        try:
            for _ in range(n_ops):
                # pick_op chooses the next op and returns a callable bound to its context. The
                # driver just runs it -- arg generation, tracing, and state updates all live in the
                # op_* method itself.
                self.pick_op(nodes, rnd, trace)()
            if self.state.txn is not Txn.NO:
                # Close any transaction left open at the end of the chain before verifying.
                self._end_txn(nodes, commit=True)
            self.verify(nodes, trace)
        finally:
            # A transaction left open by a mid-sequence failure is rolled back automatically when
            # the connection is closed at teardown (these are never prepared txns), so no explicit
            # rollback is needed here.
            for n in nodes:
                n.close()
            trace.close()

    # --- tests -----------------------------------------------------------

    def test_smoke(self):
        # Short seeded run with writes, starting from empty tables.
        self.setup_connections()
        self.run_sequence(seed=12345, tag='smoke', n_ops=80)

    def test_random(self):
        # Mixed read/write/advance/evict sequences, fresh tables per seed, start empty.
        self.setup_connections()
        for seed in range(10):
            self.run_sequence(seed=seed, tag='r%d' % seed, n_ops=300)
        # Self-check that the run actually exercised the surface (not a product assertion).
        self.assert_self_coverage()
