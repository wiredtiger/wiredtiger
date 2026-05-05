# Review: Gap Legitimacy Under `disagg=leader` Hook

**Date:** 2026-05-04  
**Scope:** All gap-analysis files in `test/analysis/03_gap_analysis/` and `test/analysis/05_scenario_analysis/`  
**Question:** Which identified gaps are already covered by running all Python tests with the `disagg=leader` hook?

---

## What `disagg=leader` Covers and Does Not Cover

`disagg=leader` runs all Python tests in leader mode, exercising **only the stable table** (reads
and writes go through the layered cursor but the stable btree is the authoritative data store).

**Covered by `disagg=leader`:**
- Basic CRUD on layered tables in leader role (insert, update, remove, search, iteration)
- Single-session transactions with timestamps in leader mode
- Checkpoints that drain ingest → stable (the normal leader checkpoint path)
- Schema operations: create, truncate (slow), verify on leader
- Statistics and connection config in leader mode
- General Python tests (HS, prepare, eviction, encryption) run through the hook on leader

**NOT covered by `disagg=leader` (user-confirmed legitimate gap categories):**
- **Checkpoint pickup** (`disagg_advance_checkpoint`, follower picking up a leader checkpoint)
- **Content in the ingest table** (between-checkpoint state, data that has never been drained)
- **Stepping up** (follower → leader role transition / drain)
- **Stepping down** (leader → follower, elegant or restart-based)
- **Any multi-node / follower operations**
- **Crash recovery** (SIGKILL + reopen scenarios)
- **Fault injection** (page log failures, schema creation failures, block layer corruption)
- **C-level tests** (csuite, catch2, cppsuite — no Python involvement)
- **Hook-skipped categories**: backup, compaction, salvage, column-store, secondary indexes,
  read-only connections, named checkpoints

---

## File-by-File Verdict

### `c_level_and_unit_test_gaps.md`

**All gaps remain legitimate.** Every gap in this file describes csuite (crash tests), catch2
(unit tests), or cppsuite (stress tests). `disagg=leader` runs only Python tests. These C-level
test suites have no Python equivalent; the gaps stand independently.

Key gaps confirmed legitimate:
- [CRITICAL-1] No crash/recovery tests for disaggregated storage (csuite) — Python tests cannot SIGKILL
- [CRITICAL-2] Zero catch2 unit tests for `block_disagg/` (7 source files, 0 unit tests)
- [HIGH-1] No disagg HS cleanup stress test (cppsuite)
- [HIGH-2] No disagg correctness stress test (cppsuite) — failover perf test asserts no invariants
- [HIGH-3] No csuite crash test for checkpoint metadata + schema operations
- [HIGH-4] No unit tests for `conn_layered_ingest.c` drain logic

---

### `disagg_block_layer.md`

**All CRITICAL and HIGH gaps remain legitimate.** They require fault injection, mocks, or
specific error conditions unreachable from normal Python operation.

| Gap | Verdict | Reason |
|-----|---------|--------|
| GAP-1: `checkpoint_resolve(failed=true)` | **Legitimate** | Requires in-process fault injection |
| GAP-2: Infinite retry loop in read_multiple | **Legitimate** | Requires page service returning 0 results |
| GAP-3: `disagg_block_read_ahead_frontier` stat | **Legitimate** | Requires `last_materialized_lsn` set + read ahead of it; leader has no frontier |
| GAP-4: Checksum/magic/version corruption in read loop | **Legitimate** | Requires injecting corrupt header bytes |
| GAP-5: `WT_BLOCK_DISAGG_MODIFIED` victim cache path | **Legitimate** | test_layered43 explicitly self-skips; feature blocked |
| GAP-6: `plh_discard == NULL` branch | **Legitimate** | Requires NULL-plh_discard mock |
| GAP-7: `is_root=true` size accounting invariant | **Legitimate** | `disagg=leader` exercises this path but no test asserts the specific double-subtraction invariant |
| GAP-8: Address cookie unpack error paths | **Legitimate** | Requires crafting malformed cookies |
| GAP-9: `pl_open_handle` failure in `__wti_block_disagg_open` | **Legitimate** | Requires mock that returns error from pl_open_handle |
| GAP-10: Block handle ref-count sharing | **Legitimate** | Exercised when two sessions open same table; assertions about ref count and single pl_open_handle call are missing |
| GAP-11: Empty checkpoint (`root_image == NULL`) at integration level | **Mostly legitimate** — the code path IS triggered by `disagg=leader` when a table is checkpointed twice with no new data, but the restart verification (load with NULL cookie after cold restart) is NOT covered by `disagg=leader` |
| GAP-12: `write_size` EINVAL overflow guard | **Legitimate** | Requires UINT32_MAX-sized buffer |
| GAP-13: `block_magic` stat not asserted | **Questionable** — if any Python test opens `statistics:layered:T` and checks `block_magic`, this could be a one-liner fix; but no existing test does this even with `disagg=leader` |
| GAP-14: Non-.wt/.wt_stable suffix else-branch | **Legitimate** | Test-only `file:` URI without suffix; not created by standard tests |

---

### `disagg_cross_cutting_features.md`

**All CRITICAL and HIGH gaps remain legitimate.** Several MEDIUM gaps are partially covered.

| Feature / Gap | Verdict | Reason |
|---------------|---------|--------|
| [CRITICAL] Backup | **Legitimate** | Hook skips all backup: cursors |
| [CRITICAL] Shared HS deep update chains + concurrent readers | **Partially covered** — basic HS tests (test_hs01–33) DO run via hook, so basic shared HS write/read/eviction is exercised in `disagg=leader`. The specific "deep update chains + concurrent readers + shared HS restart" scenario remains a gap |
| [HIGH] RTS correctness on layered tables | **Legitimate** — `disagg=leader` hook skips all `rollback_to_stable*` tests; RTS is classified NEVER for disagg (see `08_unsupported_features.md`) |
| [HIGH] Concurrent multi-session insert stress | **Legitimate** | Python tests are single-threaded |
| [HIGH] Compaction behavior (silent no-op) | **Legitimate** | Hook skips test_compact* |
| [MEDIUM] Statistics: step_up_time, database_size, role_leader, etc. | **Legitimate** | These stats require step_up/follower/role-transition scenarios not in `disagg=leader` |
| [MEDIUM] Prepared txns crossing drain boundary | **Legitimate** | Drain is a step_up operation; `disagg=leader` never triggers the drain |
| [MEDIUM] Key rotation with encryption | **Legitimate** | Requires specific key rotation event |
| [LOW] Secondary index rejection path | **Legitimate** | Hook intercepts before API; no negative test at C level |

---

### `disagg_layered_checkpoint_rts.md`

**All gaps remain legitimate.** The majority involve follower operations, RTS (unsupported),
crashes, or specific timing scenarios unavailable in `disagg=leader`.

| Gap | Verdict | Reason |
|-----|---------|--------|
| [CRITICAL] RTS on ingest btrees — silent no-op | **Legitimate** | RTS never called on disagg; recovery RTS globally skipped; classified as NEVER |
| [CRITICAL] Prepared txn spanning two checkpoint boundaries | **Legitimate** | Requires specific drain timing at step_up |
| [HIGH] Crash between page-log metadata write and local metadata update | **Legitimate** | Requires crash injection |
| [HIGH] RTS on stable btree with `WT_UPDATE_DURABLE` | **Legitimate** | Requires explicit RTS on disagg (classified NEVER) |
| [HIGH] Follower checkpoint pickup with mismatched oldest/stable | **Legitimate** | Follower operation; not in `disagg=leader` |
| [HIGH] Concurrent eviction during checkpoint — size accounting race | **Legitimate** | `disagg=leader` exercises this path normally, but the specific race in size accounting is not explicitly asserted |
| [MEDIUM] Multiple consecutive empty checkpoints — GC prune stall | **Partially covered** — `disagg=leader` DOES trigger empty checkpoints (writing nothing, then checkpointing again), but the test does NOT assert `prune_timestamp` progression; the gap is about explicit verification of GC prune behavior |
| [MEDIUM] Checkpoint with only shared-metadata-table updates | **Partially covered** — `disagg=leader` creating a table without data and checkpointing exercises this; the gap is the explicit assertion about `largest_file_id` and `database_size` |
| [MEDIUM] Prepared txn rolled back/committed concurrently during drain | **Legitimate** | Requires drain (step_up) |
| [LOW] Table drop during in-progress checkpoint | **Legitimate** | Timing-specific; requires mid-checkpoint interruption |

---

### `disagg_layered_cursor.md`

**Most gaps remain legitimate.** Key point: several gaps require ingest-table tombstones or
follower/step_up scenarios. In `disagg=leader`, the ingest table IS populated between checkpoints
but tests generally don't set up the specific adversarial conditions described.

| Gap | Verdict | Reason |
|-----|---------|--------|
| [CRITICAL] `cursor.reserve()` entirely untested | **Superseded** — test_layered92.py and test_layered93.py cover reserve × {stable-only, ingest-only, both, missing} × {leader, follower}. The remaining gap is **reserve + commit (not rollback)**, which IS a legitimate gap |
| [CRITICAL] `search_near` NEXT→PREV dual-iterate recovery | **Legitimate** | Requires specific data layout (ingest tombstone covering exact match, all neighbors above also absent); not set up by standard tests |
| [CRITICAL] `cursor.modify()` on ingest tombstone | **Legitimate** | Requires ingest tombstone; blocked by `ops.pct.modify=0` anyway |
| [HIGH] `cursor.update()` with `overwrite=false` | **Legitimate** | Standard tests use `overwrite=true`; specific tombstone-as-ingest scenario not set up |
| [HIGH] `largest_key()` returns deleted key | **Legitimate** | No standard test deletes the largest key then calls largest_key() |
| [HIGH] `cursor.bound()` propagation after checkpoint advance mid-iteration | **Legitimate** | Requires follower mid-iteration checkpoint advance |
| [HIGH] `next_random` with combined stable+ingest | **Legitimate** | Standard tests only test ingest-only next_random (test_layered22) |
| [HIGH] Positioned `cursor.remove()` during active scan | **Legitimate** | No standard test does positioned-remove-during-iteration |
| [MEDIUM] Step-down mid-transaction | **Legitimate** | Requires role transition (elegant step-down is DEFERRED) |
| [MEDIUM] `cursor.bound()` + `cursor.search()` (not search_near) | **Superseded** — test_layered82 provides comprehensive bound testing including search. This gap as stated in the *cursor.md* file is **invalidated by test_layered82** |
| [MEDIUM] `cursor.reset()` clears bounds | **Superseded** — test_layered82 tests reset+bounds. Already covered |
| [MEDIUM] Concurrent cursors mid-scan + modifier | **Legitimate** | Multi-threaded; Python tests are single-threaded |
| [MEDIUM] Tombstone added to ingest mid-iteration (read-committed) | **Legitimate** | Requires specific concurrent timing |
| [LOW] `cursor.compare()` | **Legitimate** | No standard test uses cursor.compare() |

**Summary for cursor gaps:** The `disagg_layered_cursor.md` file was written before the Pass 4
scenario analysis. Several of its gaps were refined or superseded by the `05_scenario_analysis/`
pass. The CRITICAL item about `cursor.reserve()` being "entirely untested" is **incorrect** —
test_layered92/93 cover the state-matrix for reserve. The remaining reserve gap (commit end-to-end)
is captured as CW-H7 in the scenario analysis.

---

### `disagg_layered_ingest_drain.md`

**All CRITICAL and HIGH gaps remain legitimate.** They require step_up, crash injection, or
specific timing scenarios.

| Gap | Verdict | Reason |
|-----|---------|--------|
| GAP-1: Drain under memory pressure | **Legitimate** | Requires specific cache configuration not in standard tests |
| GAP-2: Concurrent writes during drain | **Legitimate** | Drain happens at step_up; `disagg=leader` has no step_up |
| GAP-3: Error paths and partial failure | **Legitimate** | Requires fault injection |
| GAP-4: Prepared txns during drain | **Legitimate** | Requires step_up + concurrent prepare |
| GAP-5: Multithreaded drain | **Legitimate** | Requires `drain_threads > 1` configuration; no Python test sets this |
| GAP-6: Crash recovery mid-drain | **Legitimate** | Requires step_up + crash injection |
| GAP-7: Delete-heavy / all-tombstone workloads | **Legitimate** | The assertion "value exists on stable to delete" at line 82 could fire at step_up when ingest tombstones don't have stable values; requires step_up |
| GAP-8: `PRESERVE_PREPARED=false` behavior | **Legitimate** | The drain with `preserve_prepared=false` is the step_up drain path; not in `disagg=leader` |
| GAP-9: Prune timestamp update race with drain | **Legitimate** | Requires concurrent drain (step_up) and checkpoint |
| GAP-10: Lock contention and deadlock prevention | **Legitimate** | Requires concurrent schema ops during drain |

---

### `disagg_layered_role_transitions.md`

**All gaps remain legitimate.** Every gap in this file directly involves a role transition
(step_up, step_down, cold restart, or follower promotion).

| Gap | Verdict |
|-----|---------|
| [CRITICAL] Step-up failure leaves leader=true but corrupted state | **Legitimate** |
| [CRITICAL] Multiple consecutive role transitions — WT_BTREE_READONLY not cleared | **Legitimate** (SD-1 in unsupported_features.md, DEFERRED) |
| [CRITICAL] Cold restart with partially written checkpoint — pl_abandon_checkpoint no-op | **Legitimate** |
| [HIGH] Stable-table creation failure mid-loop during cold restart | **Legitimate** |
| [HIGH] Open uncommitted transactions on follower at promotion time | **Legitimate** |
| [HIGH] Follower that has never picked up any checkpoint | **Legitimate** |
| [HIGH] Checkpoint pickup TOCTOU race | **Legitimate** |
| [MEDIUM] Table drop-and-recreate — stale table manager entry | **Legitimate** |
| [MEDIUM] Connection open race with concurrent page log writes | **Legitimate** |
| [MEDIUM] Two consecutive cold restarts without new leader checkpoint | **Legitimate** |

---

### `disagg_schema_metadata_recovery.md`

**All CRITICAL and HIGH gaps remain legitimate.**

| Gap | Verdict | Reason |
|-----|---------|--------|
| [CRITICAL] Partial `__create_layered` failure — orphaned metadata | **Legitimate** | Requires fault injection on stable-file creation |
| [CRITICAL] Leader crash mid-drain during step_up | **Legitimate** | Requires step_up + crash |
| [CRITICAL] Leader crash during page-log write | **Legitimate** | Requires crash injection |
| [CRITICAL] `rename`/`alter` on `layered:` URI silently unsupported | **Partially covered** by `disagg=leader` — IF any standard test calls session.alter on a layered URI, it would hit `__wt_bad_object_type`. BUT `ops.alter=0` in CONFIG.disagg means no standard test calls alter. The **negative/behavior test** is still a gap (no test asserts the specific error). Captured as ALT-1 in `08_unsupported_features.md` |
| [HIGH] Schema ops concurrent with active drain | **Legitimate** | Requires step_up + concurrent DDL |
| [HIGH] `import` into disagg connection | **Legitimate** | Requires import attempt; no Python test does this |
| [HIGH] `verify` does not check delta-chain consistency | **Legitimate** | Requires delta corruption injection |
| [HIGH] `verify` on follower with stale/truncated page log | **Legitimate** | Requires follower + page log replacement |
| [HIGH] `alter` config not propagated to both constituents | **Legitimate** | Same as rename/alter above |
| [MEDIUM] No disagg equivalent of `schema_abort` | **Legitimate** | Requires crash injection (C-level) |
| [MEDIUM] `timestamp_abort -G` skips schema-operations thread | **Legitimate** | C-level test modification needed |

---

### `general_checkpoint_rts_hs_prepare.md`

These are **general WiredTiger gaps** (not disagg-specific). With `disagg=leader`, standard Python
checkpoint, HS, RTS, and prepare tests DO run via the hook.

| Gap | Verdict | Reason |
|-----|---------|--------|
| [CRITICAL] Crash recovery via named checkpoint | **Legitimate** | Named checkpoints not supported in disagg (NC-1 in unsupported_features.md); `disagg=leader` skips named-checkpoint calls |
| [CRITICAL] Concurrent RTS + checkpoint + eviction three-way stress | **Legitimate** | RTS classified as NEVER for disagg; hook skips RTS tests |
| [CRITICAL] HS final pass (HS itself rolled back by RTS) | **Legitimate** | Requires RTS; classified NEVER |
| [HIGH] Checkpoint callback ordering with concurrent create+drop | **Potentially covered** by `disagg=leader` if multi-table concurrent DDL exists in Python tests. But the specific concurrent-create+targeted-checkpoint scenario is not set up by standard tests. **Still a gap** |
| [HIGH] HS overflow records inside HS btree | **Legitimate** | Requires low `leaf_value_max` configuration |
| [HIGH] Checkpoint cursor overwrite EBUSY | **Legitimate** | Requires named checkpoint; skipped |
| [HIGH] Prepare + RTS + eviction during prepared window | **Legitimate** | Requires RTS; classified NEVER |
| [MEDIUM] Per-checkpoint oldest_ts enforcement under concurrent updates | **Legitimate** | Requires named checkpoint |
| [MEDIUM] Partial backup restore + RTS HS orphan truncation | **Legitimate** | Requires backup + RTS |
| [MEDIUM] High-concurrency HS write stress | **Partially covered** — `disagg=leader` runs HS tests but not with 16 concurrent sessions writing overlapping timestamps. Single-session HS tests are covered; stress variant is still a gap |
| [MEDIUM] Prepared truncation + RTS | **Legitimate** | Requires RTS; classified NEVER |
| [MEDIUM] Crash recovery tests for checkpoint16/29/30 | **Legitimate** | Those tests don't use crash simulation; `disagg=leader` doesn't add crash support |

---

### `05_scenario_analysis/` (Pass 4 — most refined)

The Pass 4 synthesis already accounts for `disagg=leader` coverage and is the authoritative source.
Key clarifications:

**Items moved to `08_unsupported_features.md` (DEFERRED or NEVER — not immediate test gaps):**
- Elegant step-down (SD-1–SD-5): DEFERRED, blocks many role-transition gap scenarios
- RTS (RTS-1–RTS-5): NEVER — write behavior tests, not functionality tests
- Table drop (DRP-1–DRP-5): DEFERRED (WT-14503)
- Fast truncate (FT-1): DEFERRED
- session.alter() (ALT-1–ALT-2): No plan
- Named checkpoints (NC-1): No plan
- session.salvage(), compact(), import(): No plan / Never
- Bulk/backup cursors: Not planned
- Column-store RECNO: Never
- Index creation: Skipped
- Prepared transactions (PT-1–PT-5): DEFERRED — disagg-specific behavior

**The one CRITICAL and 36 HIGH gaps from the synthesis remain legitimate** because:

For the HIGH cursor write gaps (CW-H1, H3, H6): The scenario descriptions include "step_up" in
the failure scenario. The single-leader code path (write → checkpoint → update same key) IS
incidentally exercised by `disagg=leader` whenever any Python test writes data and then updates
it across a checkpoint boundary. However:
1. No existing Python test explicitly sets up this scenario as a correctness test
2. The assertions about stable-base lookup are not present
3. The two-node step_up scenario (the main production risk) is not exercised at all

These remain HIGH priority gaps to write tests for.

**Specific cases worth flagging from the scenario analysis:**

| Scenario Gap ID | Covered by `disagg=leader`? | Notes |
|-----------------|----------------------------|-------|
| CS-C1 (atomic cross-table committed txn) | **Partially** — basic cross-table commits ARE exercised if standard Python tests write to 2 tables in one txn. But the crash scenario (crash between two ingest writes) is NOT exercised | Still needs explicit test + crash variant |
| CW-H1 (update stable-only key) | **Partially** — post-checkpoint update exercises this code path incidentally | Explicit test with assertions still needed |
| CW-H3 (remove stable-only key) | **Partially** — same reasoning as CW-H1 | Explicit test still needed |
| CW-H7 (reserve + commit end-to-end) | **NOT covered** — test_layered92/93 only test reserve + rollback | Gap confirmed |
| TT-H1 (read_ts < oldest_ts) | **Potentially covered** if timestamp tests try this; synthesis marks as HIGH | Verify with explicit assertion |
| TT-H2 (oldest_ts advancement + GC) | **NOT covered** — involves ingest GC pruning | Gap confirmed |
| SO-H2 (truncate on leader) | **Potentially covered** by `disagg=leader` if truncate tests run | Verify existing truncate tests run in leader mode |
| CS-H2/H3 (`disagg_block_get/put`, `disagg_role_leader` counters never asserted) | **NOT covered** — counters are incremented but never asserted by any Python test | Simple stat assertions to add |

---

## Summary: What `disagg=leader` Does NOT Cover

The following categories of gaps are **confirmed legitimate** regardless of `disagg=leader` coverage:

1. **C-level test gaps** (csuite, catch2, cppsuite) — Python runner doesn't exercise these
2. **Follower operations** — checkpoint pickup, follower cursor ops, follower eviction
3. **Role transitions** — step_up, step_down, cold restart (all DEFERRED or NEVER)
4. **Ingest table intermediate state** — data between checkpoints, tombstones in ingest
5. **Crash recovery** — SIGKILL + reopen, mid-checkpoint crashes, mid-drain crashes
6. **Fault injection** — block layer corruption, page log failures, schema create failures
7. **Hook-skipped test categories** — RTS (NEVER), backup, compaction, salvage, column-store, named checkpoints (no plan), secondary indexes
8. **Multi-threaded Python scenarios** — concurrent sessions, concurrent writes, concurrent DDL
9. **Block layer internals** — address cookie corruption, write_size overflow, ref-count race
10. **`block_disagg_read.c` error paths** — checksum mismatch, magic mismatch, retry loop
11. **All `disagg_layered_role_transitions.md` gaps** — 100% require non-leader scenarios
12. **All `disagg_layered_ingest_drain.md` gaps** — 100% require step_up or crash injection

## Gaps Where `disagg=leader` Provides Some Coverage

These gaps are **partially covered** — the code path is exercised but explicit assertions are missing,
or only the single-node scenario is covered (not the two-node production scenario):

1. Basic HS tests (HS tests run via hook) — covers HS write/read/eviction at basic level
2. Basic prepare tests (prepare tests run via hook) — covers single-session prepare scenarios
3. Basic eviction (eviction tests run via hook) — covers leader eviction scenarios
4. Update/remove on post-checkpoint (stable-only) key — code path exercised; no explicit test
5. Cross-table committed transaction atomicity — normal commit path exercised; crash scenario not

These still need explicit tests.

## Items to Reclassify or Note as Superseded

1. **`disagg_layered_cursor.md` CRITICAL: `cursor.reserve()` entirely untested** → **SUPERSEDED**
   by test_layered92.py and test_layered93.py. The remaining gap (reserve + commit) is CW-H7 in
   the scenario analysis.

2. **`disagg_layered_cursor.md` MEDIUM: `cursor.bound()` + `cursor.search()`** → **SUPERSEDED**
   by test_layered82.py comprehensive bound tests.

3. **`disagg_layered_cursor.md` MEDIUM: `cursor.reset()` clears bounds** → **SUPERSEDED**
   by test_layered82.py.

4. **`disagg_cross_cutting_features.md` RTS and named checkpoint gaps** → **RECLASSIFIED** as
   NEVER/No-plan in `08_unsupported_features.md`. Not immediate test gaps.

5. **`disagg_layered_checkpoint_rts.md` all RTS gaps** → **RECLASSIFIED** as NEVER/No-plan.
   `test_layered87.py` should be extended to assert RTS behavior (what it actually does on disagg).

6. **`disagg_schema_metadata_recovery.md` rename/alter gaps** → **RECLASSIFIED** as ALT-1/ALT-2
   in `08_unsupported_features.md`. The gap is writing a negative test, not implementing the feature.
