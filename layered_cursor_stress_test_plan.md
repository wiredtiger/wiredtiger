# Layered Cursor Stress Test — Plan & Status (v3)

Design + tech decisions: `layered_cursor_stress_test_design.md`
Implementation: `test/suite/test_layered_cursor_stress.py`
Run (from `build/`): `python3 ../test/suite/run.py test_layered_cursor_stress`

This is the single source of truth for **what we are doing, where we are, and what is
unresolved**. Tickboxes are concrete goals; check them only with real passing test output.

---

## 1. Design in one paragraph (v3)

Single-threaded, seed-reproducible. Per connection (leader + follower): one session and two
tables — **ASC = a plain (non-layered) reference table** and **DSC = the layered table under
test** — each with its own cursor. Every chosen operation is applied to the layered cursor,
then the reference cursor, and the results (error code + key + value) are compared; writes go
to both tables; transaction/timestamp ops apply at the session level. The same op sequence is
replicated to the follower connection. The plain reference is real WiredTiger, so it is a
correct oracle under read_timestamps / isolation / prepared transactions with no modeling.
Cursors are kept **positioned across long chains** (resets are rare). Scenarios (eviction,
mass delete, bulk insert, prepare floods, mid-iteration checkpoint advance) are injected at
seeded points. Tables start empty.

---

## 2. Current status

Done and green (Python prototype, v1/v2 shape — leader-vs-follower oracle):

- [x] **M0** Build + harness de-risk (existing layered tests pass).
- [x] **M1** Core loop + comparison (leader-vs-follower per op; search_near bracket check).
- [x] **M2** Mirrored writes + ingest/stable split (355 tombstone-shadow + 224 ingest-wins
      events asserted; full-scan verifier).
- [x] **M3** Wild scenarios + deterministic eviction (5 named scenarios; `release_evict`).
- [x] **Candidate bug found** and parked (see §5).

Suite state: 8 tests pass, 1 skipped (the finding). Fully reproducible.

> The v2 oracle was leader-vs-follower only. v3 adds the **plain-reference-table** oracle and
> reshapes the driver around **long-lived positioned cursors**. The comparison/eviction/
> scenario infrastructure carries over; the main rewrite is replacing reset-on-mutation with
> per-cursor position tracking and adding the reference table.

---

## 2b. Review checkpoint (go/no-go) — after Phase C

The "main key concepts" are: (1) the independent reference oracle [Phase A — DONE], (2)
long-lived positioned-cursor chains [Phase B], (3) transactions / timestamps / isolation /
prepare [Phase C]. Once Phase C lands the core engine is complete; Phases D (scenarios), E
(config matrix), F (shrinking/CI) are additive breadth on the established framework.

**=> STOP after Phase C for a full-system review and a go/no-go decision before investing in
D/E/F.** Deliver a full implementation presentation that reads from scratch to the deepest
detail:
- `layered_cursor_stress_test_architecture.md` — multi-altitude guide: (a) one-paragraph mental
  model; (b) topology (connections / sessions / tables / cursors / oracle); (c) the per-op
  control flow + the temporal-drain lifecycle; (d) the invariants (lockstep, position parity,
  merge-exercised, reproducibility); (e) the subtle mechanics with code refs (drain recipe,
  `oldest=stable`, snapshot pinning, `search_near` alignment, the resolved finding); (f) a
  concepts->code map and a how-to-extend / how-to-review guide.
- a `.dot` topology + op/lifecycle diagram referenced from the doc.

## 3. v3 build plan (tickboxes)

### Phase A — Reference-table oracle + foundation refactor  (implemented; review pending)
- [x] A1. Reference-table placement: one plain `table:` per connection; the follower hosting a
      writable plain table is confirmed working (the test writes to it on the follower).
- [x] A2. Per connection: plain `table:` ASC reference + shared `layered:` DSC table; one read
      cursor + one write cursor on each, all in one session (the `Node` helper).
- [x] A3. `apply()` runs the read op on the layered cursor and the reference cursor; returns
      both normalized `(ret, key, value, cmp)`.
- [x] A4. `compare_read()` checks error code + key + value (layered vs reference) per node; a
      mismatch reports the trace path; `verify()` cross-checks leader-layered vs follower-layered.
- [x] A5. search_near alignment: equidistant neighbours must bracket the key and be adjacent;
      step the reference cursor exactly one `next`/`prev` onto the layered key to re-sync.
- [x] A6. Writes mirrored to both tables on both connections in one timestamped txn; `advance`
      folds leader stable -> follower stable (dirty-flag guarded to skip no-op advances).
- [~] A7. NOTFOUND handling done (search/next/prev return NOTFOUND, compared, cursor reused).
      `WT_DUPLICATE_KEY` (needs `overwrite=false`) DEFERRED to Phase E1 (overwrite config),
      where it belongs — noted so it is not lost.
- [x] A7b. **Writes unified with reads (minimal).** Writes go through the SAME cursors as reads
      (no separate write cursors); the write return code is captured and compared
      layered-vs-reference (`mirror_write`). Cursors are no longer reset before put/remove (only
      before advance/evict), so position carries per WT semantics. Full long-lived positioned
      chains (positional update/remove, rare resets) remain Phase B.
- [x] A11. `compare_search_near` now checks the layered `cmp` sign vs the search key in all
      branches (0 exact / <0 smaller / >0 larger).
- [x] A12. `test_read_only` logs a `# seed=N` trace note at each seed boundary; per-`run_seed`
      tests already write one trace file per seed.
- [~] A13. Write return codes are now captured (explicit `insert()`/`remove()`) and compared
      layered-vs-reference; an unexpected rollback-required code surfaces at commit. A strict
      `== 0` assert is deferred (it becomes legitimate non-zero under `overwrite=false`/prepare).
- [x] A8. Reproducibility + event tracing: seed-driven (`SEED=` printed; `STRESS_SEED` env
      replays one seed); every event appended to a per-seed trace file, flushed each step
      (verified: `WT_TEST/.../stress_trace_<tag>_<seed>.txt`). Trace path reported on failure.
- [x] A9. Green: smoke + read-only + random fuzz + checkpoint-delete regression. Reproducible
      across runs; `STRESS_SEED` override works.

#### Phase A review findings (independent agent) — Phase A NOT done until A10 fixed
- [x] A10. **DONE — critical blocker fixed.** The follower now genuinely merges stable + ingest
      (measured: 659 stable / 909 ingest follower reads, reproducible), with a guard that FAILS
      if follower `*_stable` reads stay at zero. **Deterministic drain recipe** (found by reading
      `conn_layered_ingest.c:1069-1074`): a checkpoint's data is prunable only when a NEWER
      checkpoint exists (`ckpt_inuse < last_ckpt`), so the recipe is: set `oldest_timestamp`
      (= stable) on advance, take **≥2 checkpoints** with the target keys in the OLDER one, then
      `release_evict` the ingest constituent URI (`file:NAME.wt_ingest`). The older keys are
      pruned from ingest → read from stable; keys in the last checkpoint stay in ingest → a scan
      merges them. (`precise_checkpoint=true` broke tearDown verify and was NOT needed; the
      layered-URI evict did not prune — the ingest URI does.) `test_read_only` builds this
      explicitly and asserts the merged scan == reference (verifies the mixed temporal case the
      eviction investigation never tested).
- [ ] A11. (MEDIUM) `compare_search_near` bracket branch does not check the layered `cmp` sign
      vs the search key — a correct neighbour key with a wrong `cmp` sign would pass. Add it.
- [ ] A12. (LOW) `test_read_only` shares one trace file mislabeled `seed=0` across 10 seeds; add
      a per-seed boundary note and the real seed in the name/header.
- [ ] A13. (LOW) Assert write return codes (`remove`/`insert`) are 0 in `mirror_write` (matters
      once Phase C introduces prepare/rollback).

#### Decision: unify writes with reads (folds Phase B in)
Per Ivan: writes should be applied to the SAME layered+reference cursors and compared like reads
(return code + post-write position + `DUPLICATE_KEY` existing value), not mirrored via separate
write cursors with reset-on-write. This adds write-result coverage AND yields long-lived
positioned cursors naturally (the Phase A write path was a v2 carry-over that diverged from the
design). Apply in the next pass together with A10–A13.

#### A10 investigation — make stable a live read source (parallel worktree agents)
Goal: make the follower genuinely read from stable (non-zero `layered_curs_*_stable` stats)
while keeping a CONTROLLABLE amount of ingest retained — NOT always empty after a checkpoint
(per Ivan: want options for how much ingest stays). Each agent builds in its own worktree,
experiments, measures follower stable-vs-ingest read stats, and reports: (1) does stable become
a read source, (2) can the ingest-retained fraction be controlled, (3) is it deterministic /
seed-reproducible, (4) recommendation. Boxes ticked as agents report.

Agent E — explicit eviction cursor (option b) — DONE:
- [x] I-E1. `release_evict` cursor on the follower (layered URI `layered:NAME` OR ingest URI
      `file:NAME.wt_ingest`) drops the ingest page; evicted keys then read from stable (confirmed
      via stats: search_ingest 200->0, search_stable 0->200; values stay correct).
- [x] I-E2. Per-key partial eviction is IMPOSSIBLE within one table: the `in_memory` ingest is a
      single leaf page that never splits, so evict is all-or-nothing per table. A precise stable
      *fraction* needs N tables (evict k of N) — but that yields single-constituent tables (no
      merge within a cursor).
- [x] I-E3. No cache-size limit needed; `release_evict` is synchronous and works at any cache size.
- [x] I-E4. Fully seed-deterministic (same seed -> same evicted set -> same stable-served set).

**KEY INSIGHT (changes A10 design): the merge is exercised by a TEMPORAL split, not a partition.**
Lifecycle: write batch -> advance -> evict ingest (batch now stable-only) -> write fresh keys
(-> ingest). A scan then interleaves stable (old) + ingest (new) = real merge, and re-writes /
removes of old keys give overlap / tombstone-shadow cases. The "% retained in ingest" knob is
the ratio of keys-written-since-last-evict (ingest) to evicted-old-keys (stable), controlled by
batch/evict cadence. This is the production lifecycle (write->checkpoint->evict drains ingest).

Agent N — natural eviction (cache size + scale-up) — DONE:
- [x] I-N1. Small cache works only when working set > cache, and is non-monotonic (10MB→0.96,
      25MB→0.59, 50MB→0.82, 100MB→0.21, 1GB→0.0; 5MB hangs).
- [x] I-N2. Scale-up shows a hard ON/OFF threshold at working-set-vs-cache; MORE advances did
      not raise the fraction; 0 advances → always 0% (prune-timestamp gate).
- [x] I-N3. NON-deterministic: 5 identical 50MB runs spanned 0.72–0.86. Direction reliable,
      magnitude not — breaks seed reproducibility (our #1 requirement).
- [x] I-N4. Coarse, not a dial; sharp threshold, non-monotonic, plus a cache floor that hangs.
- Mechanism: an ingest entry drops only when (1) `prune_timestamp` advanced (only on
  `disagg_advance_checkpoint`, no cursor holding old checkpoints) AND (2) the ingest leaf is
  reconciled (on a follower, only via eviction). The background prune path needs
  `precise_checkpoint=true` on the follower (FIXME-WT-14721, not auto-enabled by disagg).

### DECISION (both agents agree): explicit eviction cursor + temporal split
- Primary: `debug=(release_evict)` on the follower's layered/ingest URI — deterministic
  (stable_frac=1.0, reproducible, cache-independent, fast). Drive the merge via the TEMPORAL
  lifecycle (write → advance → evict → write fresh), so a scan interleaves stable (old) +
  ingest (new). The "% retained in ingest" knob = keys-written-since-last-evict vs evicted.
- Verify during A10 whether `precise_checkpoint=true` is needed on the follower for the
  forced-evict prune to drop ingest entries (add it if so — harmless).
- Reserve natural eviction (small cache / scale-up) for ONE optional coverage test with a
  qualitative assertion only (`stable_reads > 0`), never as the deterministic mechanism.
- Then implement A10 + a guard that FAILS if follower `*_stable` reads stay at zero.

### Phase B — Long-lived positioned cursors  (IMPLEMENTED; review pending)
- [x] B1. Per-op position semantics documented (`layered_cursor_position_semantics.md`) and
      encoded in `pick_op` (which ops keep vs clear position).
- [x] B2. Chain generator favours position-keeping ops when positioned (next/prev/pos_update/
      pos_remove dominate); set_key put/remove and reset are rare. Observed: chains up to ~20
      consecutive position-holding ops; positional update/remove genuinely exercised (18/17 in
      a sample seed).
- [x] B3. `self.cur_pos` tracks the positioned key; positional ops are gated to live keys.
- [x] B4. 10 seeds × 300 ops green, reproducible; guard asserts positional ops ran
      (`n_positional > 0`) and the merge stays exercised.
- [x] B5. Positional update/remove go through the *positioned* write path (2164 branch), valid
      via a `search` that re-establishes the position inside the write txn. The anticipated
      tombstone-shadow divergence did NOT materialise for live positioned keys (layered ==
      reference). Note: true "iterate-and-delete in one transaction" (positioning + write in
      the same txn) is Phase C.

#### Finding (Phase B): positional remove across a transaction boundary — candidate bug (ticket?)
A positional `remove()` issued in a *new* transaction after the positioning `next()` committed
hits `__clayered_remove_follower:2166-2167` deriving `positioned` from the iface `KEY_INT`
(which survives a commit) but then reading the *ingest constituent's* `VALUE_INT`/value (which
does NOT survive the txn switch). Reviewer's analysis: that check is a `WT_ASSERT`
(diagnostic-only, compiled out in release) — so in a **release build** there is no abort and
`__wt_clayered_deleted(&value)` runs on stale/undefined constituent state → **silent
misbehavior**, on a usage the public `remove()` contract permits. Recommendation: file a ticket
(re-validate the constituent or return an error rather than assert-or-trust-stale-value).
PENDING Ivan's decision to file. The stress test avoids it by re-positioning inside the write
txn; the genuine iterate-and-delete-in-one-transaction case lands in Phase C.

**Timestamp-model reframing (verified):** layered writes are **always timestamped by design** —
the layered table AND both constituents are created with `log=(enabled=false)`
(`schema_create.c:1186-1215`, "...to ensure we have timestamps"), and disagg rejects
`write_timestamp_usage=never` (`schema_create.c:1599-1605`); the ordered timestamp-usage check
then requires a commit_timestamp on any write whose key was previously timestamped
(`txn_inline.h:647-652`). So a positional write MUST run in an explicit timestamped txn, and the
**supported pattern is position + write in the SAME txn** (the doc contract
`arch-disagg-layered-cursor.dox:42-45`; the test's re-search-in-txn does exactly this, so it is
the *correct* pattern, not a hack). Consequences: (i) the autocommit "always use timestamps"
rejection is EXPECTED, not a bug; (ii) Q1 is best classed as **unsupported cross-txn usage that
fails UNSAFELY** (diagnostic assert / release stale-value read) rather than a bug in the
supported flow — the gap is defensive handling: `__clayered_remove` enters with `reset=false`
(`cur_layered.c:2422`) and never re-validates, so the fix is to re-read the constituent (the
`__clayered_lookup` path the unpositioned branch already uses) or return a clean error.
**Severity — RESOLVED EMPIRICALLY (release build, asserts off):** the cross-txn positional
remove (with a commit_timestamp) is a **clean, CORRECT success** in release — `remove()` → 0,
commit succeeds, key correctly removed (identical to the same-txn control). NOT silent
corruption. The stale `c->value` buffer still held the live value, so the
`__wt_clayered_deleted` short-circuit (`cur_layered.c:2174`) was not taken; it proceeded to the
real tombstone update and the timestamp check passed. The autocommit no-timestamp case (Path A)
correctly raises `EINVAL` and leaves data unchanged (NOT swallowed). So Q1 is NOT a correctness
bug; it is **(1) an overly-strict diagnostic-only `WT_ASSERT`** that aborts a sequence release
handles correctly (so diagnostic/CI builds crash on a legal timestamped cross-txn positional
remove), plus **(2) a latent defensive-coding hazard**: `WT_ITEM_SET(value, c->value)` reads the
ingest value with no `VALUE_INT` guarantee — harmless here, but would misfire (wrong
`WT_NOTFOUND`) if that buffer ever held the 2-byte tombstone marker. Cleanest fix addresses
both: when `VALUE_INT` isn't set, re-read via `__clayered_lookup` instead of asserting + trusting
the buffer.

#### FINDINGS — RESOLVED (Ivan's verdicts)

**Q1 = REAL BUG. Fixed in branch `wt-17796-fix-cross-txn-positioned-remove` (WT-17796).**
A positional `remove()` in a new txn after the positioning read committed reads the ingest
constituent's value buffer when `VALUE_INT` is not set (the `WT_ASSERT` at
`cur_layered.c:2166-2167` correctly flags this invariant violation). Diagnostic aborts; release
reads an unguaranteed buffer (correct only by luck of leftover contents). The WT team confirmed
and fixed it; our empirical "release happened to work" was the lucky case, not a refutation.
Repros: `findings/repro_q1_cross_txn_positional_remove.py`, `findings/repro_q1_release_behavior.py`.

**Q2 = NOT a bug — standard read-committed snapshot behavior, identical for ASC and DSC.**
A *held* scan cursor pins the read-committed snapshot it took when iteration began, so it keeps
seeing the pre-delete view (key 110 live) across its `next()` calls; a *separate* point
`search()` takes a NEWER snapshot and sees the later ingest tombstone (`WT_NOTFOUND`). Scan and
search read at DIFFERENT snapshots — NOT the same one (my earlier "same-snapshot inconsistency"
framing was wrong). Plain read-committed semantics: a non-layered cursor behaves identically,
and under `isolation=snapshot` the divergence disappears (one txn snapshot). Repro:
`findings/repro_q2_pinned_scan_search.py`.

**Why the ASC-vs-DSC oracle did NOT (and cannot) find Q2:** the oracle applies the SAME op to
the layered (DSC) and reference (ASC) cursors, both opened on the SAME session (`compare_read`),
so for any one operation they read under the SAME snapshot and agree — snapshot pinning hits
both equally. Q2 never came from the oracle; it came from (a) the M3 scenario, which held a
follower cursor across an advance and compared its old-snapshot reads against fresh reads, and
(b) the standalone repro, which compares a held SCAN (old snapshot) vs a fresh SEARCH (new
snapshot). Those are held-vs-fresh / different-snapshot comparisons, not ASC-vs-DSC. **The
same-session oracle is therefore SOUND — immune to snapshot-pinning false positives by
construction.** (Reinforces the design rule: always compare layered vs reference in the same
session/snapshot.)

- **G1** = `search_near_stable` counter impurity — already FIXME-WT-15545; the guard already
  excludes it. No repro.
- The other 9 accommodations are standard WT cursor/disagg-lifecycle semantics (search_near
  either-neighbour, drain via ingest URI, ≥2-checkpoint prune gate, `oldest=stable`,
  reset-before-advance, ignored redundant-checkpoint warning, `precise_checkpoint` non-adoption,
  forward-looking write-rc compare, the merge floor) — cited contracts, no repro warranted.

### Phase C — Transactions, timestamps, isolation
- [x] C1. begin/commit/rollback applied at session level; cursors survive a txn switch
      mid-chain (commit/rollback in the middle, not just at the end). **DONE + REVIEWED**
      (independent agent: APPROVE WITH NITS; all 5 design-intent points confirmed, txn state
      machine / timestamp monotonicity / both cross-txn guards / rollback restore all verified).
      Review fixes applied: aggregate coverage guards (assert_merge_exercised, n_positional>0)
      now skipped under single-seed `STRESS_SEED` replay (they false-failed seed 9 on the 10%
      stable-read floor — a multi-seed coverage heuristic, not a per-chain invariant); finally
      now rolls back a txn left open by a mid-chain failure; `live_snapshot` cleared after use.
      - `pick_op` is now txn-aware: `begin` only in autocommit; `commit`/`rollback` only with a
        txn open; advance/evict (checkpoint lifecycle) only in autocommit. A txn left open at
        the end of a chain is committed before `verify`.
      - Writes (`mirror_write`) and positional writes (`apply_positional`) participate in the
        open txn when `in_txn` (bare cursor ops; one shared `commit_timestamp` applied at the
        commit op), else keep the per-op autocommit begin/commit. `_end_txn` handles commit
        (bump ts + commit at it iff the txn wrote; cursors stay positioned) vs rollback
        (rollback both sessions, restore `self.live` from a begin-time snapshot, cursors reset).
      - **Genuine same-txn iterate-and-delete (the Phase B gap):** an in-txn positional write is
        DIRECT (no re-search) — the positioning read and the write share the transaction, so the
        cursor is truly positioned (KEY_INT|VALUE_INT) and update/remove operate on the current
        position. Autocommit positional writes still re-search (WT-17796 cross-txn hazard).
      - **Two cross-txn hazards fixed during C1:**
        (a) `begin` clears the generator's `cur_pos` — a positional write off a pre-txn position
        would be the WT-17796 cross-txn positioned-remove; the physical cursor stays positioned
        so next/prev keep iterating across the switch, but a positional WRITE must be
        re-established by an in-txn read first.
        (b) `cur_pos` is now anchored only when leader AND follower ended on the **same** key.
        A DIRECT in-txn positional write operates on each cursor's current position; `search_near`
        may legitimately land leader and follower on different (both valid) neighbours, which
        would diverge the two reference tables. (Found by the oracle on first run, seed r1 —
        not a product bug, the documented search_near neighbour non-determinism across the
        ingest+stable split vs stable-only leader.)
      - Evidence (10 random seeds × 300 ops, green): 127 begins / 83 commits / 43 rollbacks;
        85 in-txn DIRECT positional writes; merge + n_positional guards still hold.
- [x] C2. `read_timestamp` variants — reference (plain WT) and layered must agree on the
      as-of-T view. **DONE + REVIEWED** (independent agent: APPROVE — verified the oldest/stable
      state machine against the engine invariants in `txn_timestamp.c`, zero read-ts-below-oldest
      violations, byte-identical determinism, the regression's non-vacuity, and that as-of-T
      reads hit a populated merge so a read_timestamp merge bug would surface; two doc nits
      applied).
      - PREREQUISITE done: `advance()` no longer pins `oldest == stable`. `stable` moves to the
        latest commit; `oldest` LAGS one advance behind (`oldest = max(1, last_advance_ts)`,
        monotonic, `< stable`), keeping `[oldest, latest]` open for as-of-past reads. Verified
        the ingest drain + merge guard still hold under the lagged oldest (suite green).
      - `begin` has two flavours: read-write (C1) or, when a past window exists, a **read-only
        as-of-T** txn (`read_timestamp` = random point in `[oldest, latest]`). `self.txn_read_ts`
        gates the generator to reads-only inside such a txn; `pick_op` weights next/prev on raw
        positioned-ness there (cur_pos may sit on a key absent from current `self.live` — it was
        live at T). On end, an as-of-T txn clears `cur_pos` (its position is in a historical view).
      - Oracle: every read inside the txn is compared layered-vs-reference as-of-T (both are real
        WT honouring `read_timestamp`), so a layered read_timestamp merge bug surfaces as a
        mismatch. Coverage guard `n_read_ts > 0` (multi-seed; skipped under single-seed replay).
      - Deterministic regression `test_scenario_read_timestamp_history`: a key overwritten across
        two checkpoints (old version in stable history, ingest drained) reads back the OLD value
        at the old commit's ts and the NEW value at the new commit's ts — on both tables.
      - Evidence: 5 tests green; 53 as-of-T read txns / 77 read-write txns across 10 seeds;
        single-seed replays (9, 3, 7) green.
- [ ] C3. Isolation levels: snapshot / read-committed / read-uncommitted.
- [ ] C4. Multi-session prepared transactions left pending → drive `WT_PREPARE_CONFLICT`
      deterministically; both tables must report it identically.
- [ ] C5. Transaction-level error recovery: on `WT_PREPARE_CONFLICT` / `WT_ROLLBACK`, roll back
      the transaction and then keep reusing the same cursor (clean state); compare recovery
      behaviour layered-vs-reference. (Cursor-level `WT_NOTFOUND`/`WT_DUPLICATE_KEY` reuse is A7.)

### Phase D — Scenario injections (at seeded points)
- [ ] D1. Evict 20/40/60/80/100% of ingest mid-cursor-life (`release_evict`).
- [ ] D2. Mass delete via `remove()` (→ all-tombstones follower state) AND via `truncate()`
      (fast-truncate path); the "all tombstones + 1 inserted key" edge.
- [ ] D3. Sudden bulk insert to grow the table.
- [ ] D4. Checkpoint advance mid-iteration (positioned cursor across advance — the M3 area).
- [ ] D5. Adjacent-key insert next to the cursor position, then `next` (snapshot visibility).
- [ ] D6. Tombstone-byte-prefixed value (`__clayered_deleted_encode` E-2 edge).

### Phase E — Config matrix (seed-selected; separate and combined)
- [ ] E1. `overwrite` on/off (per cursor open).
- [ ] E2. `bounds` on/off via `bound()` (incl. narrowing/widening; cleared on reset).
- [ ] E3. Combine config dims with B/C/D under one seed.

### Phase F — Debuggability + CI
- [ ] F1. Delta-debug shrinking of a failing op sequence to a minimal repro.
- [ ] F2. Register in `test/evergreen_disagg.yml`; run N seeds; docstring on reproducing a seed.
- [ ] F3. `dist/s_all` clean.

---

## 4. Open problems / unresolved points

- **O1 (RESOLVED).** A `role=follower` connection CAN hold a writable plain ASC table
  (confirmed by Ivan). One reference table per connection.
- **O2 (RESOLVED).** Per-op position semantics documented in
  `layered_cursor_position_semantics.md`. Summary for the chain generator:
  KEEP position → `search`, `search_near`, `next`, `prev`, `update`, `modify`, `reserve`, and
  `remove` *if already positioned* (stays on the removed key, KEY_INT, no value). CLEAR
  position → `insert` (always), `remove` when unpositioned (set_key+remove), `reset`,
  `largest_key`. Layered keeps `ITERATE_NEXT/PREV` across a positioned `update`/`modify`/`remove`
  so a write inside a scan does not restart iteration; clears them on
  `insert`/`reserve`/`search`/`search_near`/`largest_key`.
- **O3 (RESOLVED — not a bug).** A worktree config-bisect found the controlling factor is
  read-committed **snapshot pinning** (a held cursor → `ncursors > 0`, or an explicit
  txn/`read_timestamp`): while pinned, the follower's stable constituent does not advance to a
  new checkpoint mid-iteration, so a held cursor keeps the pre-delete view (correct semantics).
  Unpinned → scan == search == correct. Converted to passing regression
  `test_scenario_checkpoint_delete_visible`. Carry-forward: the v3 oracle compares layered vs
  plain reference in the SAME session/snapshot, and pinned-snapshot-across-advance is where
  layered vs plain-table may legitimately differ — surface for domain judgment, do not assume
  a bug. See §5 and the finding memory.
- **O4 (resolved).** ASC/DSC meaning — ASC = plain reference table, DSC = layered table, one
  cursor each, compare every op.
- **O5 (resolved).** Oracle scope — plain-reference-table primary + leader-vs-follower
  secondary.
- **O6 (resolved).** Framework — Python now; test/model (C++) later; never hand-roll MVCC.
- **O7 (resolved).** Threading — single-threaded, multi-session.

---

## 5. Finding — RESOLVED (not a bug: read-committed snapshot pinning)

The follower "iteration shows deleted keys / disagrees with point search after a checkpoint
advance" was traced (worktree config-bisect) to correct **read-committed snapshot pinning**.
While a session's snapshot is pinned — another cursor held open/positioned (`ncursors > 0`) or
an explicit txn / `read_timestamp` — the follower's stable constituent does NOT advance to the
new checkpoint during iteration (`__clayered_can_advance_stable` false while
`WT_TXN_HAS_SNAPSHOT`; iteration enters via `__clayered_enter(reset=false)`; the snapshot
releases only at `ncursors==0`). So a held cursor legitimately keeps the pre-delete checkpoint
view. `cache_size` / `statistics` / `oldest_timestamp` had zero effect. With nothing pinning
(fresh session, single cursor) → scan == search == correct.

Converted to passing regression `test_scenario_checkpoint_delete_visible` (unpinned case);
suite green, no skips.

**v3 carry-forward (Phase A):** compare the layered cursor vs the plain reference in the SAME
session/snapshot. A held cursor / explicit txn across a checkpoint advance keeps the old view —
do not treat that as a mismatch. Pinned-snapshot-across-advance is where layered vs plain-table
may legitimately differ; surface it for Ivan's domain judgment rather than assuming a bug.
Details in memory `finding-stale-checkpoint-cursor`.
