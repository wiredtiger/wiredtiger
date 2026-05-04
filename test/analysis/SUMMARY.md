# WiredTiger Test Coverage Gap Analysis — Executive Summary

> May 2026 · Covers: disaggregated storage (primary focus) + general WT subsystems  
> Detailed findings: `03_gap_analysis/` · Methodology: `NOTES.md`  
> **Pass 4 update:** See `05_scenario_analysis/00_synthesis.md` for the current master gap list with corrections below applied.

---

## ⚠ Pass 4 Corrections (May 2026)

The following items in this document (from Pass 3) have been corrected based on the official
"Unsupported WT Features in Disagg" spec and source-level verification:

| Pass 3 entry | Correction |
|---|---|
| "Two consecutive role swaps: `WT_BTREE_READONLY` set on step-down" listed as CRITICAL | **DEFERRED** — Step-down is only supported via server restart (elegant step-down targets Public Preview). `test_layered_double_role_swap` cannot be written until then. |
| "RTS skip path for disagg connections … explicit `rollback_to_stable()` on disagg could incorrectly roll back ingest data" listed as CRITICAL | **Reclassified** — RTS is "Never" supported in disagg. The test goal is a **negative/behavior test** (what does `conn.rollback_to_stable()` do?), not an RTS correctness test. See UN-1 in `05_scenario_analysis/00_synthesis.md`. |
| "`rename`/`alter` on `layered:` URI — falls through to `__wt_bad_object_type`" | **Partially correct** — `session.alter()` exists but is "No plan" for disagg; need a negative test. `session.rename()` does **not exist** as a `WT_SESSION` method in this codebase (`schema_rename.c` does not exist). |
| P1 item 9: `test_layered_double_role_swap` | **DEFERRED** — see above |
| P2 item 12: `test_layered_explicit_rts` | **Reclassified** — reframe as behavior test (UN-1), not correctness test |
| P3 item 23: `test_layered_rts_correctness` | **Remove** — RTS is Never supported; no correctness test is appropriate |

---

## Scope

Three-pass analysis of WiredTiger's full test suite:
- **Pass 1** — Suite inventory: 22 test directories surveyed (`02_suite_analysis/test_suites_overview.md`)
- **Pass 2** — Per-test analysis: ~900 individual test files read and documented (`01_per_test_analysis/`)
- **Pass 3** — Gap analysis: 10 deep analyses of source code vs test coverage (`03_gap_analysis/`)

Primary focus: **disaggregated storage** (`src/block_disagg/`, `src/conn/conn_layered*.c`, `src/cursor/cur_layered.c`).

---

## CRITICAL Gaps — Disaggregated Storage

These gaps represent untested failure modes with direct data-loss or availability consequences in production.

### Block Layer (`src/block_disagg/`)
> Full analysis: [disagg_block_layer.md](03_gap_analysis/disagg_block_layer.md)

| Gap | Risk | Proposed test |
|---|---|---|
| `checkpoint_resolve(failed=true)` never tested — failed checkpoint could silently commit corrupted shared metadata for all nodes | data corruption | `test_disagg_checkpoint_resolve_failure` |
| LSN frontier check (`__block_disagg_check_lsn_frontier`) never asserted — broken guard allows reads past materialization frontier | follower inconsistency | `test_disagg_frontier_enforcement` |
| Corruption detection (checksum/magic/version mismatch) never tested — no guard against corrupt pages from page service | silent corrupt data | `test_disagg_read_corruption` |
| Infinite retry loop in `__block_disagg_read_multiple` (FIXME WT-15768) never exercised — could hang or return stale buffer | hang / stale read | `test_disagg_read_retry` |

### Cursor Operations (`src/cursor/cur_layered.c`)
> Full analysis: [disagg_layered_cursor.md](03_gap_analysis/disagg_layered_cursor.md)

| Gap | Risk | Proposed test |
|---|---|---|
| `cursor.reserve()` completely untested on layered tables — follower path uses `overwrite=true` even when key exists only in stable | silent write-lock failure | `test_layered_cursor_reserve` |
| `cursor.modify()` on an ingest tombstone — wrong base value used as modify delta base | silent data corruption | `test_layered_cursor_modify_tombstone` |
| `search_near` dual-iterate recovery with unpositioned cursor at exhaustion boundary | wrong result returned | `test_layered_cursor_search_near_recovery` |

### Role Transitions (`src/conn/conn_layered.c`)
> Full analysis: [disagg_layered_role_transitions.md](03_gap_analysis/disagg_layered_role_transitions.md)

| Gap | Risk | Proposed test |
|---|---|---|
| `__disagg_step_up` sets `leader=true` before operations that can fail — flag never rolled back on error | permanent corrupt connection state | fault-injection in `test_layered_stepup_fault` |
| Two consecutive role swaps: `WT_BTREE_READONLY` set on step-down, never cleared on second step-up — re-promoted leader cannot write | replica permanently read-only after two swaps | `test_layered_double_role_swap` |
| Cold restart with partially-written checkpoint — `pl_abandon_checkpoint` is a no-op for some PALI backends (FIXME-WT-16524) | crashed checkpoint corrupts delta chain | crash-injection in `test_layered_cold_restart_partial_ckpt` |

### Ingest-Drain Mechanism (`src/conn/conn_layered_ingest.c`)
> Full analysis: [disagg_layered_ingest_drain.md](03_gap_analysis/disagg_layered_ingest_drain.md)

| Gap | Risk | Proposed test |
|---|---|---|
| No test for copy→truncate failure — stable + ingest both contain data after partial drain, undefined semantics on restart | data duplication / data loss | `test_layered_drain_errors` |
| No crash injection mid-drain — no persistent drain-in-progress marker means crash leaves permanent inconsistency | permanent inconsistency after crash | `test_layered_drain_crash_recovery` |
| `drain_threads > 1` completely untested — deadlock between workers possible; hang blocks replica promotion | replica promotion hangs | `test_layered_drain_multitable` |

### Checkpoint / Timestamp / RTS Interactions
> Full analysis: [disagg_layered_checkpoint_rts.md](03_gap_analysis/disagg_layered_checkpoint_rts.md)

| Gap | Risk | Proposed test |
|---|---|---|
| RTS skip path for disagg connections (`txn_recover.c:1355`) has zero test coverage — explicit `rollback_to_stable()` on disagg could incorrectly roll back ingest data | data loss via incorrect RTS | `test_layered_explicit_rts` |
| Prepared transaction spanning two checkpoint boundaries — drain filtering at `conn_layered_ingest.c:404` unvalidated for this boundary | prepared data silently dropped or duplicated | `test_layered_prepare_spanning_checkpoint` |
| Crash between `plh_put` (page-log write) and local metadata update — page log ahead of local view after restart | split-brain metadata | crash-injection in `test_layered_checkpoint_meta_crash` |

### Schema, Metadata, and Crash Recovery
> Full analysis: [disagg_schema_metadata_recovery.md](03_gap_analysis/disagg_schema_metadata_recovery.md)

| Gap | Risk | Proposed test |
|---|---|---|
| Partial `__create_layered` failure — no rollback of already-inserted metadata entries → `WT_ASSERT_ALWAYS` on next leader open | crash loop on startup | fault-injection in `test_layered_create_partial_failure` |
| Leader crash mid-drain — `leader=true` set before drain completes (FIXME-WT-14734), crash here loses all ingest data silently | silent data loss | crash-injection variant of drain tests |
| `rename`/`alter` on `layered:` URI — falls through to `__wt_bad_object_type` with no documented contract | undefined caller behavior | `test_layered_schema_rename_alter` |

### Cross-Cutting Features (Backup, HS, RTS, Eviction)
> Full analysis: [disagg_cross_cutting_features.md](03_gap_analysis/disagg_cross_cutting_features.md)

| Gap | Risk | Proposed test |
|---|---|---|
| **Backup has zero disagg integration** — `hook_disagg.py` skips all 30 `test_backup*` tests; page log has no backup API | disagg databases unbackable | new `test_disagg_backup` suite |
| **Shared HS (`WiredTigerSharedHS.wt_stable`) tested by only one test** — none of the 33 `test_hs*` tests use disagg; disagg HS statistics never asserted | HS bugs invisible on disagg | disagg-mode variants of `test_hs*` |
| RTS skips all `rollback_to_stable` named tests via hook; RTS correctness on disagg only exercised incidentally by `format CONFIG.disagg` | disagg RTS correctness unverified | `test_layered_rts_correctness` |

---

## CRITICAL Gaps — General WT (Non-Disagg)

> Full analysis: [general_checkpoint_rts_hs_prepare.md](03_gap_analysis/general_checkpoint_rts_hs_prepare.md)

| Gap | Risk | Proposed test |
|---|---|---|
| **Named checkpoint recovery never tested** — opening a database with `checkpoint=(name=X)` at recovery has zero Python test coverage | named checkpoint recovery broken | `test_checkpoint_named_recovery` |
| **`__wti_rts_history_final_pass` never directly targeted** — HS rolled back by RTS itself; only reached as side effect | HS final-pass RTS logic uncovered | `test_rollback_to_stable_hs_final_pass` |
| RTS + checkpoint + eviction three-way stress never exercised simultaneously | heavy-load crash recovery broken | cppsuite RTS stress variant |

---

## CRITICAL Gaps — C-Level and Unit Tests

> Full analysis: [c_level_and_unit_test_gaps.md](03_gap_analysis/c_level_and_unit_test_gaps.md)

| Gap | Risk | Proposed test |
|---|---|---|
| **Zero csuite crash/recovery tests for disagg** — no WAL means crash semantics differ fundamentally from regular B-trees | crash corner cases completely untested | new `csuite/disagg_abort/` csuite test |
| **Zero catch2 unit tests for `src/block_disagg/`** — 7 source files, 0 unit tests | block-layer regressions caught only at integration level | new `catch2/block_disagg/` unit tests |
| `conn_layered_ingest.c` drain logic has no catch2 unit tests despite active modification (appears in `git status`) | drain regressions caught only at integration level | unit tests for `__layered_copy_ingest_table` |

---

## Top Duplicate / Consolidation Candidates

> Full analysis: [test_suite_duplicates.md](03_gap_analysis/test_suite_duplicates.md)

| Action | Test(s) | Reason |
|---|---|---|
| **Delete immediately** | `test_layered43` | 100% skipped at runtime (FIXME-WT-15663) — provides zero coverage |
| **Remove** | `test_layered18` | Strict subset of `test_layered20` (10 vs 32 delta rounds) |
| **Remove** | `test_layered36` | Fully covered by `test_layered30`'s `another_table=True` scenario |
| **Merge** | `test_rollback_to_stable16` | Source file admits redundancy; absorbed by RTS01 + RTS15 |
| **Merge** | `test_checkpoint10` + `test_checkpoint11` | Identical except timestamps; add `use_timestamps` param |
| **Merge** | `test_checkpoint18` + `test_checkpoint19` | Identical HS-DS pairing test (non-TS vs TS) |
| **Merge** | `test_checkpoint24` + `test_checkpoint25` | Identical fast-delete-in-checkpoint-cursor (non-TS vs TS) |
| **Merge** | `test_hs10` | Essentially Phase 1 of `test_hs08` |
| **Trim** | `test_hs06` | Two near-identical methods; merge into one parametrized method |

---

## Implementation Roadmap

### P0 — Fix immediately (no new test needed, bugs in existing code)
- Remove `test_layered43` (permanently skipped, dead code)
- Remove `test_layered18` (duplicate of `test_layered20`)
- Remove `test_layered36` (duplicate of `test_layered30`)
- Document or test `rename`/`alter` on `layered:` URI (currently falls through to bad-object-type error with no contract)

### P1 — New tests: CRITICAL disagg gaps
Priority order based on production risk:

1. `test_layered_drain_errors` — copy→truncate failure recovery (GAP in ingest-drain)
2. `test_layered_drain_crash_recovery` — SIGKILL mid-drain, verify restart correctness
3. `csuite/disagg_abort/` — timestamp_abort analog for disagg (no crash/recovery exists)
4. `catch2/block_disagg/` — unit tests for address cookie, checkpoint cookie, corruption detection
5. `test_disagg_checkpoint_resolve_failure` — `checkpoint_resolve(failed=true)` path
6. `test_disagg_frontier_enforcement` — LSN frontier guard assertion
7. `test_disagg_read_corruption` — checksum/magic mismatch detection
8. `test_layered_stepup_fault` — fault injection during step-up (leader flag rollback)
9. `test_layered_double_role_swap` — two consecutive leader→follower→leader transitions
10. `test_layered_cursor_reserve` — `cursor.reserve()` on layered table

### P2 — New tests: HIGH disagg gaps

11. `test_layered_drain_multitable` — `drain_threads > 1` configuration
12. `test_layered_explicit_rts` — explicit `rollback_to_stable()` on disagg connection
13. `test_layered_prepare_spanning_checkpoint` — prepared txn across two checkpoint boundaries
14. `test_layered_cursor_modify_tombstone` — `cursor.modify()` on ingest tombstone
15. `test_layered_cold_restart_partial_ckpt` — crash with partial page-log checkpoint
16. `test_layered_create_partial_failure` — fault injection during `__create_layered`
17. `test_disagg_backup` — backup integration for disagg databases

### P3 — New tests: MEDIUM gaps and general WT

18. `test_checkpoint_named_recovery` — named checkpoint recovery
19. `test_rollback_to_stable_hs_final_pass` — direct coverage of `__wti_rts_history_final_pass`
20. `test_layered_drain_memory_pressure` — drain under small-cache / eviction pressure
21. `test_layered_drain_prepared_txns` — concurrent commit/rollback during drain fix-up
22. `test_layered_drain_tombstones` — all-tombstone ingest drain
23. `test_layered_rts_correctness` — RTS per-key verification on disagg tables
24. Disagg-mode variants of `test_hs*` — shared HS correctness under HS tests
25. `test_layered_checkpoint_meta_crash` — crash between `plh_put` and local metadata update

---

## Files in `03_gap_analysis/`

| File | Source analyzed | Key finding count |
|---|---|---|
| [disagg_block_layer.md](03_gap_analysis/disagg_block_layer.md) | `src/block_disagg/*.c` | 4 critical, 5 high |
| [disagg_layered_cursor.md](03_gap_analysis/disagg_layered_cursor.md) | `src/cursor/cur_layered.c` | 3 critical, 5 high, 7 medium |
| [disagg_layered_role_transitions.md](03_gap_analysis/disagg_layered_role_transitions.md) | `src/conn/conn_layered.c` | 3 critical, 4 high |
| [disagg_layered_ingest_drain.md](03_gap_analysis/disagg_layered_ingest_drain.md) | `src/conn/conn_layered_ingest.c` | 2 critical, 2 high, 6 medium |
| [disagg_layered_checkpoint_rts.md](03_gap_analysis/disagg_layered_checkpoint_rts.md) | `conn_layered_page_log.c` + RTS source | 2 critical, 4 high |
| [disagg_schema_metadata_recovery.md](03_gap_analysis/disagg_schema_metadata_recovery.md) | Schema, verify, crash recovery | 4 critical, 4 high |
| [disagg_cross_cutting_features.md](03_gap_analysis/disagg_cross_cutting_features.md) | Backup, HS, RTS, eviction in disagg | 2 critical, 3 high, 6 medium |
| [c_level_and_unit_test_gaps.md](03_gap_analysis/c_level_and_unit_test_gaps.md) | csuite, catch2, cppsuite | 2 critical, 4 high |
| [general_checkpoint_rts_hs_prepare.md](03_gap_analysis/general_checkpoint_rts_hs_prepare.md) | Checkpoint, RTS, HS, prepare | 3 critical, 4 high |
| [test_suite_duplicates.md](03_gap_analysis/test_suite_duplicates.md) | Full 748-file Python suite | 10 consolidation groups |
