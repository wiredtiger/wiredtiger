# Layered cursor stress test — architecture overview

Implementation: `test/suite/test_layered_cursor_stress.py` (two random-generated test methods —
`test_smoke` and `test_random` — over a shared driver). This document is the multi-altitude map of *how it works*; the companion diagram
is `layered_cursor_stress_test_architecture.dot` (rendered `.svg`/`.png`). For the phase-by-phase
status and findings see `layered_cursor_stress_test_plan.md`; for the design rationale see
`layered_cursor_stress_test_design.md`.

---

## 1. What is under test, and the core idea

**Under test:** the follower **layered cursor** in `src/cursor/cur_layered.c`. On a disaggregated
follower a layered table has two constituents — an in-memory **ingest** btree (recent local
writes) and a **stable** checkpoint (pulled from the leader). Every read must **merge** the two:
ingest shadows stable, ingest tombstones hide stable keys, iteration skips ingest tombstones,
checkpoint advances reposition the stable cursor, prepared ingest updates raise conflicts. That
merge has many subtle branches and is the bug surface.

**Core idea — differential testing against a real-WiredTiger oracle.** Instead of modelling MVCC
in Python, every logical operation is applied to *two* tables that must agree:

- **DSC** — the **layered** table under test (`layered:...`), which on the follower runs the merge.
- **ASC** — a plain (non-layered) **reference** table (`table:...`), ordinary WiredTiger.

Both live in the **same session**, so they share one snapshot/isolation/read-timestamp. The plain
table is correct by construction for timestamps, isolation, and prepare — so any layered-vs-plain
divergence is a layered bug (or a documented, legitimate layered/plain difference we surface for
judgement). Operation sequences are **random but seed-reproducible**.

---

## 2. Topology (see the diagram)

Two connections to the same disaggregated cluster, opened in `setup_connections` / `make_nodes`:

- **Leader** (`self.conn` / `self.session`) — role=leader. Its layered cursor reads **stable only**
  (skips ingest), so it behaves like a plain table and is a second oracle.
- **Follower** (`self.conn_follow` / `self.session_follow`) — role=follower. Its layered cursor
  **merges ingest + stable** — this is the code actually being stressed.

Per connection a **`Node`** bundles, in one session, a layered cursor (`lay_c`) and a reference
cursor (`ref_c`) on a shared-named layered table and a per-connection plain table. The same cursors
serve **both reads and writes**, so they stay in lockstep and a write leaves the cursor positioned
(feeding the long-lived positioned-cursor chains, §6).

Keys are integers spread on a grid (`POOL = 100,110,…,990`) so `search_near` targets can fall in
gaps; values are unique strings (`new_value`).

---

## 3. The two oracles

1. **Per-operation, per-node (the workhorse).** `compare_read` runs each read op on `lay_c` then
   `ref_c` of the *same* node and compares the normalized `(ret, key, value, cmp)`. Same session →
   same snapshot → apples-to-apples, immune to read-committed snapshot-pinning false positives.
   `search_near` is special-cased (`compare_search_near`): WT may return *either* immediate
   neighbour of an absent key, so the rule is (a) the layered `cmp` sign must match the returned
   key vs the search key, and (b) if the two cursors land on different valid neighbours they must
   *bracket* the search key and be exactly one step apart (the reference is stepped onto the
   layered key to re-sync).
2. **Whole-table, cross-node (at `verify`).** After a chain: each node's layered full scan must
   equal its reference scan, **and** the leader's layered scan must equal the follower's layered
   scan (same logical data reached two different ways — stable-only vs merged).

Writes are also result-compared (`_write_pair` / `_positional_pair` assert the layered and
reference return codes match) — a write checked like a read.

**Guiding principle (test inventory).** The seed-driven stress test is the *only* layered-vs-regular
agreement checker in this file. A standalone scenario test earns its place **only** if it pins a
known *mismatch* (a bug repro); a scenario that merely re-asserts correct/agreeing behavior is
redundant with the oracle and is not kept. (That's why the file currently has just the two stress
tests — the agreeing scenarios were removed, and the one diverging scenario lives in the bug-review
package.)

---

## 4. Write protocol & replication model

Every logical write is **mirrored to both connections** (`mirror_write`): leader layered (→ its
stable), leader reference, follower layered (→ its **ingest**), follower reference — all at one
commit timestamp. This is the real follower path (committed data arrives in ingest locally; the
leader's copy becomes stable via checkpoint). Writes are **always timestamped** (layered tables
reject `write_timestamp_usage=never`), so each runs in a begin/commit (autocommit) or joins an open
explicit transaction (committed later at one timestamp).

**Checkpoint lifecycle** (`advance`): set `stable_timestamp = latest commit`, but `oldest_timestamp`
**lags** one advance behind (so a window `[oldest, latest]` stays open for as-of-past reads),
checkpoint the leader, and `disagg_advance_checkpoint` the follower — folding the leader's stable
into the follower's stable.

**Ingest drain** (`drain_ingest`): force-evict the follower's `…​.wt_ingest` file (a
`debug=(release_evict)` cursor) so already-checkpointed keys are pruned out of ingest and later
reads are served from **stable**. This needs ≥ 2 checkpoints (the prune horizon lags one
checkpoint). Without the drain the follower would answer everything from ingest — a degenerate
oracle that never exercises the merge (guarded against, §8).

---

## 5. Determinism, tracing, replay

An integer **seed** drives a `random.Random(seed)` (printed `SEED=<n>`). The seed set is **fixed**
(`range(10)`), so a run is fully deterministic and a failure repeats on re-run. Every chosen op is
appended to a per-seed **trace file** (`open_trace` / `EventTrace`), flushed each line, so a failure
is a self-contained record; to dig into one failing seed, run it in a throwaway test calling
`run_sequence()` (there is intentionally no global single-seed replay knob). No multithreading —
multiple **sessions** in one thread create prepared-conflict scenarios without breaking
reproducibility.

---

## 6. Operation generation — one declarative workload-shape config

The whole operation mix is **one table** (`OPS`, a list of `Op` dataclass rows) plus two dials:
- `P_BREAK` — when doing a *data* op, the chance it BREAKS the cursor position. Small ⇒ long
  position-holding chains (the heart of the test). The central bias lives in this one number.
- `P_TXN` — the chance of a transaction-control op when one is legal; **orthogonal** to `P_BREAK`
  so a low break rate never starves transactions.

Each `Op` carries a `weight`, a `category` (`keep` holds position / `break` resets it / `txn`),
and legality tags (`read_only_ok`, `needs_position`, `is_write`, `autocommit_only`, `in_txn_only`).
`op_legal(op, mode, positioned)` is the single legality predicate; `_mode(allow_writes)` derives the
mode (`read_only` / `autocommit` / `rw_txn` / `ro_txn`).

`pick_op` then: filter `OPS` to the legal ops for the current `(mode, positioned)`; with probability
`P_TXN` pick a txn-control op (if any are legal), else pick the `break` bucket with probability
`P_BREAK` and the `keep` bucket otherwise, and weighted-sample within it; bind the op's argument and
return a **bound zero-arg callable** that traces the choice and runs the op. `run_sequence` just calls
it. The legality the dials/tags encode (unchanged from the old per-mode branches):

| mode | legal ops |
|---|---|
| read-only test (`allow_writes=False`) | reads + reset + full_scan |
| autocommit (no txn open) | reads + writes + positional (if positioned) + advance/evict + begin + reset + full_scan |
| explicit read-write txn (snapshot) | reads + writes + positional (if positioned) + commit/rollback + reset + full_scan |
| explicit read-only txn (as-of-T / read-committed / read-uncommitted) | reads + commit/rollback + reset + full_scan |

**Adding an op = one `Op` row + one `op_<name>` method.** The driver (`run_sequence`) and the
op methods track `cur_pos` (the key both cursor pairs sit on) and `state.live` (the logical
key→value map, used **only** to choose keys, never as the oracle).

---

## 7. Transactions, timestamps, isolation, prepare (Phase C)

- **Explicit transactions** (`begin`/`commit`/`rollback`, `_end_txn`): writes join the open txn and
  commit atomically at one timestamp; rollback restores `self.live` from a begin-time snapshot and
  resets the cursors. Cursors **survive** a commit (position retained) — exercising mid-chain txn
  switches.
- **In-txn positional writes are DIRECT** (no re-search): the positioning read and the write share
  the transaction, so the cursor is genuinely positioned — the real same-transaction
  iterate-and-delete. In autocommit, positional writes **re-search** inside the write txn (the
  positioning value is not valid across the implicit txn boundary — WT-17796).
- **`read_timestamp` / as-of-past** (C2): a snapshot txn may read at a past timestamp in
  `[oldest, latest]`; the layered merge must reconstruct the historical view, checked against the
  reference. (Requires the lagged `oldest`.)
- **Isolation** (C3): `begin` picks snapshot / read-committed / read-uncommitted. Only snapshot
  permits writes; the other two are read-only (the engine rejects their writes). Single-threaded,
  all three return identical reads, so this exercises the distinct read paths with the oracle
  proving self-consistency; the *observable* differences need concurrency (prepare, C4).
- **Prepare** (C4/C5): explored via deterministic scenarios where a second follower session holds a
  prepared txn whose update lands in the follower **ingest**, shadowing a committed stable value;
  reads must surface `WT_PREPARE_CONFLICT` exactly as the plain table does. Point search matched
  (oracle held); forward iteration **diverged** — that became a bug candidate. The prepare scenarios
  have since been **removed from this file** (the agreeing ones were redundant with the stress
  oracle; the diverging one is a tracked bug). Prepare now lives only in the bug-review package
  (`test/suite/test_layered_prepare_iterate_diff.py` + `findings/`); it is **not** in the random
  chain — folding it in is a follow-up.

---

## 8. Anti-degeneracy coverage guards (multi-seed)

The oracle can pass *trivially* if the test never actually exercises the merge or the interesting
ops. `assert_self_coverage` bundles these as a **self-check** run after the multi-seed `test_random`
(a meta-assertion that the *test* did its job — a failure means a coverage regression, not a product
bug). The checks:

- `assert_merge_exercised` — the follower must read from **stable** a minimum fraction of follower
  reads (else it's an ingest-only degenerate oracle). Reads the `layered_curs_*_{stable,ingest}`
  stats. **Floor is an interim 1%** — `TODO(merge-coverage)`: stable reads are inherently rare in
  the random run (writes keep keys in ingest); the real fix is a forced-eviction scenario op + a
  long run (~300k ops), then restore a meaningful floor.
- `n_positional > 0` — positional update/remove chains actually ran.
- `n_read_ts > 0`, `n_iso_rc > 0`, `n_iso_ru > 0` — as-of-past, read-committed, read-uncommitted
  txns all actually fired.
- `n_full_scan > 0` — the weighted `full_scan` (verify-as-op) actually fired.

---

## 9. Coverage map

| dimension | covered | where |
|---|---|---|
| ingest+stable merge (reads) | ✅ | the whole test; `assert_merge_exercised` |
| ingest tombstone shadows stable | ✅ | random removes (`test_random`) |
| long-lived positioned chains | ✅ | `pick_op` bias; `n_positional` guard |
| search_near neighbour semantics | ✅ | `compare_search_near` |
| checkpoint advance + drain | ✅ | `advance` / `drain_ingest` (the `advance`/`evict` ops in the random run) |
| explicit txns, cursor survives switch | ✅ (C1) | `_end_txn`, `run_sequence` |
| same-txn iterate-and-delete | ✅ (C1) | in-txn DIRECT positional writes |
| `read_timestamp` / as-of-past | ✅ (C2) | `begin` read_ts in the random run; `n_read_ts` guard |
| isolation levels | ✅ (C3) | `begin` config; iso guards |
| prepared-txn conflict + recovery | ⚠️ NOT in this file | prepare is not in the random chain; lives in the bug-review `test/suite/test_layered_prepare_iterate_diff.py` + `findings/`. Folding prepare into the random chain is a follow-up. |
| scenario injections (mass delete, truncate, bulk insert) | ⏳ Phase D | — |
| config matrix (overwrite, bounds) | ⏳ Phase E | — |
| shrinking + CI wiring | ⏳ Phase F | — |

**Findings:** Q1 cross-txn positioned remove = real bug (WT-17796, fixed). Q2 pinned-snapshot
scan-vs-search = not a bug (read-committed semantics). Prepare-iterate divergence = **likely a
real bug, under review** (`findings/prepare_iterate_bug_candidate.md`).

---

## 10. Function map — bottom-to-top reading order (for the walkthrough)

Grouped low-level → high-level. We'll walk these in roughly this order.

**A. Module-level primitives + the workload config**
- `sign` · `EventTrace` (`log`/`note`/`close`) · `Node` (`__init__`/`reset_all`/`close`) ·
  `State` (`__init__` global fields / `new_sequence` per-sequence fields) ·
  the workload shape: `Op` dataclass · `OPS` table · `P_BREAK` / `P_TXN` dials · `op_legal`

**B. Cluster & table setup**
- `conn_config` · `setup_connections` (builds `self.state = State()`) · `make_nodes` · `new_value`

**C. Write protocol**
- `_write_pair` · `mirror_write` · `_positional_pair` · `apply_positional` · `_end_txn`

**D. Checkpoint lifecycle**
- `advance` · `drain_ingest`

**E. Stats / anti-degeneracy (self-checks)**
- `follower_read_split` · `assert_merge_exercised` (1% interim floor, TODO) · `assert_self_coverage`

**F. Read application & the oracle**
- `apply` · `compare_read` · `compare_search_near` · `fail_mismatch`

**G. Verification**
- `scan` · `verify`

**H. The operations (one method per op)**
- `_do_read` (shared read+compare+cur_pos) · `op_search`/`op_search_near`/`op_next`/`op_prev`/
  `op_reset` · `op_put`/`op_remove` · `op_pos_update`/`op_pos_remove` · `op_begin`/`op_commit`/
  `op_rollback` · `op_advance`/`op_evict` · `op_full_scan`

**I. Operation generation**
- `_mode` · `pick_op` (config-driven; returns a bound callable) · `pick_search_key`

**J. The driver**
- `open_trace` · `run_sequence` (loop = `pick_op(...)()`)

**K. Tests (top level)** — only seed-driven, random-generated stress tests. A standalone /
hand-built scenario is kept **only** if it pins a known layered-vs-regular *mismatch* (none do, so
there are none).
- `test_smoke` (fast canary: 1 seed × 80 ops) · `test_random` (10 seeds × 300 ops + `assert_self_coverage`)

---

## 11. Build & run

```
# build dir configured with -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1
cd build
python3 ../test/suite/run.py test_layered_cursor_stress          # both tests (deterministic)
```
