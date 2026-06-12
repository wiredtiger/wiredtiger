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

1. **Per-operation, per-node (the workhorse).** `_read` runs each read op (a cursor callable) on
   `lay_c` then `ref_c` of the *same* node and compares the normalized `(ret, key, value, cmp)`. Same
   session → same snapshot → apples-to-apples, immune to read-committed snapshot-pinning false
   positives. `search_near` is special-cased (`_read_near` / `_compare_near`): WT may return *either*
   immediate
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

## 6. Operation generation — one structured, inheritable weights config

Two pieces: an **op table** (`self.ops`, `OpSpec` rows from `_build_ops()`) carrying behaviour +
legality, and a **weights config** (`DEFAULT_WEIGHTS`) carrying the whole workload shape in one
nested structure.

Each `OpSpec` carries a **direct bound-method reference** (`fn`, e.g. `self.op_next`) — there is no
string op-name dispatch anywhere — a config-key `name` (used *only* to look up the op's weight, never
to branch), and legality tags (`needs_position`, `needs_live`, `is_write`, `autocommit_only`,
`in_txn_only`). `_legal(spec, positioned)` is the single legality predicate, pure off the tags and
`State.txn` (the current `Txn` context).

`DEFAULT_WEIGHTS` is a nested dict. A top-level entry is either a **leaf op weight** (`'next': 40`)
or a **group** `{'weight': <share at the top level>, <child>: <weight within the group>, ...}`. The
rule: a group's `weight` is how often that group wins against the other top-level entries; a child's
weight is its share *given* the group was chosen. Crucially a group contributes **exactly** its
`weight` to the top-level pool — `_candidates` distributes it over the (normalised) children, so a
group's internal scale (e.g. `commit: 70`) never leaks into the comparison against leaf ops. Two
groups today:
- `txn` (`weight` 8) — when no txn is open it resolves to a single `begin` candidate (op_begin then
  picks its flavour from the begin-mode sub-weights `snapshot`/`read_committed`/`read_uncommitted`/
  `read_timestamp`); when a txn IS open it resolves to `commit`/`rollback` by their sub-weights.
- `scenarios` (`weight` 12) — rare checkpoint/eviction (`advance`/`evict`; later: injected scenarios).

Position-HOLDING ops (reads + positional writes) carry the bulk of the weight so chains stay long
(the cursor is positioned ~36% of picks); position-BREAKING ops are deliberately light. There is no
longer a single `P_BREAK` knob — the break frequency is implicit in the weights (decision D1, a
`workload-tuning` TODO). A test inherits `DEFAULT_WEIGHTS` and may pass `run_sequence(weights=...)`,
deep-merged via `merge_weights`, to reshape part of the workload without restating it.

`pick_op`: `_candidates(positioned)` walks the config, legality-filters, and returns
`(effective_weight, spec)` pairs; it weighted-samples one and returns `lambda: spec.fn(...)` — a
**bound zero-arg callable**. Each `op_*` method owns its own argument generation, `trace.log`, and
state update; `run_sequence` just calls the callable. The legality the tags encode:

| Txn context | legal ops |
|---|---|
| `Txn.NO` (autocommit) | reads + writes + positional (if positioned) + advance/evict + begin + reset + verify |
| `Txn.SNAPSHOT` (read-write txn) | reads + writes + positional (if positioned) + commit/rollback + reset + verify |
| read-only txn (`READ_TIMESTAMP` / `READ_COMMITTED` / `READ_UNCOMMITTED`) | reads + commit/rollback + reset + verify |

**Adding an op = one `OpSpec` row in `_build_ops()` + one weight entry + one `op_<name>` method.** The driver
(`run_sequence`) and the op methods track `cur_pos` (the key both cursor pairs sit on) and
`state.live` (the logical key→value map, used **only** to choose keys, never as the oracle).

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
- `n_verify > 0` — the weighted `op_verify` (verify-as-op) actually fired.

---

## 9. Coverage map

| dimension | covered | where |
|---|---|---|
| ingest+stable merge (reads) | ✅ | the whole test; `assert_merge_exercised` |
| ingest tombstone shadows stable | ✅ | random removes (`test_random`) |
| long-lived positioned chains | ✅ | `pick_op` bias; `n_positional` guard |
| search_near neighbour semantics | ✅ | `_read_near` / `_compare_near` |
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
- `sign` · `Txn` enum (the transaction context) + `write_allowed(txn)` · `OpSpec` dataclass (the op
  row: `fn` bound-method ref + config-key `name` + legality tags) · `DEFAULT_WEIGHTS` (the nested
  weights config) + `merge_weights` (deep-merge for inheritance) · `EventTrace` (`log`/`close`) ·
  `Node` (`__init__`/`reset_all`/`close`) · `State` (`__init__` global fields / `new_sequence`
  per-sequence fields, incl. `txn`)

**B. Cluster & table setup**
- `conn_config` · `setup_connections` (builds `self.state = State()`, `self.ops = _build_ops()`,
  `self.ops_by_name`) · `_build_ops` (the `OpSpec` table) · `make_nodes` · `new_value`

**C. Write protocol**
- `_write_pair` · `mirror_write` · `_positional` (callable-based; in-txn direct / autocommit re-search) ·
  `_end_txn`

**D. Checkpoint lifecycle**
- `advance` · `drain_ingest`

**E. Stats / anti-degeneracy (self-checks)**
- `follower_read_split` · `assert_merge_exercised` (1% interim floor, TODO) · `assert_self_coverage`

**F. Read application & the oracle**
- `_anchor` · `_read` (do-callable + normalize + compare + anchor) · `_near` · `_read_near` ·
  `_compare_near` · `fail_mismatch`

**G. Verification**
- `scan` · `verify`

**H. The operations (one self-contained method per op — each owns arg-gen + `trace.log`)**
- `op_next`/`op_prev`/`op_search`/`op_search_near` · `op_reset` · `op_put`/`op_remove` ·
  `op_pos_update`/`op_pos_remove` · `op_begin`/`op_commit`/`op_rollback` · `_checkpoint` helper +
  `op_advance`/`op_evict` · `op_verify`

**I. Operation generation (no string dispatch)**
- `_legal` (pure predicate over the `OpSpec` tags + `State.txn`) · `_candidates` (walks
  `DEFAULT_WEIGHTS`, normalises group shares, legality-filters) · `pick_op` (samples a candidate,
  returns `lambda: spec.fn(...)`) · `pick_search_key`

**J. The driver**
- `open_trace` · `run_sequence` (loop = `pick_op(nodes, rnd, trace)()`)

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
