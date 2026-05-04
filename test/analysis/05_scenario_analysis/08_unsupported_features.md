# Unsupported Features — Negative Test Coverage

**Source:** "Unsupported WT Features in Disagg" spec, May 2026  
**Purpose:** Collect all gaps for WiredTiger features that are not supported (or not yet supported) in disaggregated storage. The test goal for each item is **not** to verify functionality, but to confirm the correct error behavior or document the actual behavior of the code path.

Items are grouped by feature. Each has a support status and a suggested minimum test.

---

## 1. Elegant Step-Down (Restart-Only Today)

**Support status:** Partially / Public Preview  
**Constraint:** `conn.reconfigure('disaggregated=(role="follower")')` without server restart is **not supported today**. Step-down is done via server restart. Elegant step-down (in-process reconfigure to follower) is targeted for Public Preview.

**Impact on test gaps:** Any test scenario that requires calling `reconfigure(role="follower")` in a Python test is **untestable until elegant step-down lands**. These gaps are deferred, not dropped — they become HIGH priority once the feature ships.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| SD-1 | DEFERRED | `leader → step_down → step_up` without intermediate follower checkpoint pickup — `WT_BTREE_READONLY` never cleared (FIXME-WT-14545); silent data loss in release builds, assertion crash in debug builds | `test_layered_double_role_swap.py` — implement when elegant step-down lands |
| SD-2 | DEFERRED | Rapid `step_down + step_up` — minimal repro of SD-1 (just two `reconfigure()` calls) | `test_layered_rapid_role_swap.py` |
| SD-3 | DEFERRED | Multi-hop transitions: leader→follower→leader→follower (4+ cycles) — accumulated READONLY/OUTDATED state | `test_layered_multi_hop.py` |
| SD-4 | DEFERRED | `conn.reconfigure(role="follower")` while a write transaction is open — ingest READONLY set under active writer | `test_layered_reconfig_active_txn.py` |
| SD-5 | DEFERRED | `conn.reconfigure(role="follower")` concurrent with active drain | `test_layered_reconfig_drain.py` |

---

## 2. Rollback to Stable (RTS)

**Support status:** Never  
**Rationale:** The server addresses rollback-on-step-down by rolling back to the previous checkpoint, not by using `rollback_to_stable()`.

**Note:** `test_layered87.py` exists and calls `conn.rollback_to_stable()` directly (it does not have "rollback_to_stable" in its name so it is not skipped by `hook_disagg.py:377`). The question is: what does this call actually do? The test should be extended to assert the observed behavior explicitly.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| RTS-1 | MEDIUM | What does `conn.rollback_to_stable()` actually do on a disagg connection? Confirm: no-op, error, or partial rollback? `test_layered87.py` calls it but does not assert the outcome. | Extend `test_layered87.py` with explicit assertion |
| RTS-2 | MEDIUM | RTS behavior when ingest-only data (never checkpointed) exists — what happens to ingest data? | `test_layered_rts_behavior01.py` |
| RTS-3 | MEDIUM | RTS behavior when prepared transactions are in-flight | `test_layered_rts_behavior02.py` |
| RTS-4 | LOW | RTS behavior when no `stable_timestamp` has ever been set | extend `test_layered87.py` |
| RTS-5 | LOW | `session.rollback_to_stable()` vs `conn.rollback_to_stable()` — same behavior? Does session-level work at all? | `test_layered_rts_behavior03.py` |

---

## 3. session.alter()

**Support status:** No plan (WT_SESSION::alter not supported in disagg)  
**Note:** `hook_disagg.py` has a `session_alter_replace` stub (lines 237–239) that rewrites the URI but no test ever calls it. `ops.alter=0` in `CONFIG.disagg` correctly excludes alter from format tests.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| ALT-1 | MEDIUM | Confirm correct error (e.g. ENOTSUP or WT_ERROR) when `session.alter('layered:X', ...)` is called | `test_layered_alter_negative.py` |
| ALT-2 | LOW | Confirm `ops.alter=0` in `CONFIG.disagg` is the correct setting and matches the actual behavior | Verify via ALT-1 test |

---

## 4. Named Checkpoints

**Support status:** No plan (not used by the server)  
**Note:** `hook_disagg.py:249–250` calls `skip_test()` whenever `'name='` appears in checkpoint config. No test ever reaches the named-checkpoint code path, and no negative test confirms the error code.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| NC-1 | MEDIUM | Confirm error code returned when `session.checkpoint('name=x')` is called on a disagg connection | `test_layered_named_checkpoint_negative.py` |

---

## 5. session.salvage()

**Support status:** No plan (FIXME-WT-14740)  
**Note:** `hook_disagg.py` skips salvage tests. No negative test confirms the error code.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| SAL-1 | LOW | Confirm error code returned when `session.salvage('layered:X', ...)` is called | `test_layered_salvage_negative.py` |

---

## 6. session.compact()

**Support status:** Never (compaction optimizes local file layout; irrelevant for disagg)  
**Note:** `hook_disagg.py` skips `test_compact*` tests entirely.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| CMP-1 | LOW | Confirm `session.compact('layered:X', ...)` is a no-op or returns a specific error | `test_layered_compact_negative.py` |

---

## 7. session.import()

**Support status:** No plan  
**Note:** `session.create(uri, 'import=(enabled=true,...)')` is skipped by hook.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| IMP-1 | LOW | Confirm error when `import=(enabled=true)` config is passed to a `layered:` URI | `test_layered_import_negative.py` |

---

## 8. Bulk Cursors

**Support status:** Not planned (WT-14563)  
**Note:** `open_cursor(uri, 'bulk')` is skipped by hook. No negative test.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| BLK-1 | LOW | Confirm ENOTSUP or similar when `session.open_cursor('layered:X', None, 'bulk')` is called | assert in any existing test |

---

## 9. Backup Cursors

**Support status:** Never (backup uses a completely different mechanism for disagg)  
**Note:** All `test_backup*` tests are skipped by hook.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| BAK-1 | LOW | Confirm error when `session.open_cursor('backup:', ...)` is called on a disagg connection | small negative test |

---

## 10. key_format=r (RECNO / Column Store)

**Support status:** Never (column store not supported in disagg)  
**Note:** `hook_disagg.py:271` silently downgrades RECNO tables to non-layered when `key_format=r` is detected. This silent downgrade is undocumented — no test asserts it explicitly.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| REC-1 | LOW | Document and assert that `key_format=r` tables are silently downgraded to non-layered by the hook; add an explicit assertion rather than relying on silent behavior | add assertion in existing hook test |

---

## 11. Index Creation on Layered Tables

**Support status:** Skipped (WT-14563 area)  
**Note:** `session.create('index:layered_table:idx', ...)` is skipped by hook.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| IDX-1 | LOW | Confirm `session.create('index:...')` on a layered-table parent returns an appropriate error | small negative test |

---

## 12. session.rename()

**Support status:** Not implemented (API does not exist)  
**Note:** Source inspection confirmed `schema_rename.c` does not exist and `WT_SESSION` has no `rename` method in this codebase. This is not a "test gap" but a missing capability.

| ID | Priority | Description | Suggested Action |
|----|----------|-------------|-----------------|
| REN-1 | MEDIUM | Investigate: is `session.rename()` intentionally absent, or was it removed/never ported to disagg? File a Jira ticket to either implement it or document it as explicitly unsupported. | Engineering investigation |

---

## 13. Table Drop (session.drop)

**Support status:** Not currently supported; targeted for Public Preview (WT-14503)
**Note:** `session.drop('layered:X')` is not currently supported in disaggregated storage. The `hook_disagg.py` does not explicitly skip drop calls — tests that call `session.drop()` on a layered URI may run but exercise an unsupported code path. WT-14503 tracks the work to implement proper drop support.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| DRP-1 | DEFERRED | Drop while drain is in flight — dhandle refcount handling | `test_layered_drop_concurrent_drain.py` — implement when drop lands |
| DRP-2 | DEFERRED | Drop with an active follower cursor — use-after-free risk | `test_layered_drop_active_follower.py` |
| DRP-3 | DEFERRED | Drop table with only ingest data (never checkpointed) — metadata cleanup | `test_layered_drop_unflushed.py` |
| DRP-4 | DEFERRED | Create-drop-recreate with same URI — stale metadata cleanup | `test_layered_schema_cycle.py` |
| DRP-5 | DEFERRED | Drop with active transactions (dhandle EBUSY behavior) | extend drop tests with open transaction |

---

## 14. Fast Truncate (session.truncate — fast path)

**Support status:** Not currently supported; targeted for Public Preview  
**Note:** Fast truncate is the WiredTiger optimization that marks entire pages as deleted without reading individual keys. It is distinct from *slow truncate*, which IS currently supported in disagg (Private Preview). The existing `test_layered_fast_truncate01.py`, `test_layered_fast_truncate02.py`, and `test_layered_fast_truncate03.py` exercise the fast-truncate code path on layered tables — these tests are testing an unsupported feature. Slow-truncate scenarios (full-table URI form, truncate on leader, truncate of stable-only data) remain valid test gaps since slow truncate is supported.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| FT-1 | DEFERRED | Fast-truncate range on a layered table — verify correct behavior or error | update `test_layered_fast_truncate01-03` once fast truncate lands |
| FT-2 | LOW | Confirm that slow truncate is the active code path when `session.truncate()` is called on a layered table today (not fast truncate) | add assertion / log-check to existing slow-truncate tests |

---

## 15. Prepared Transactions (Disagg-Specific Behavior)

**Support status:** Not currently supported; targeted for Public Preview  
**Context:** Prepared transactions work differently in disaggregated storage. The new disagg-specific guarantee — that prepared content is included in a checkpoint if it adheres to timestamp rules — is not yet implemented (per the May 2026 unsupported features spec). The basic prepare/commit/rollback API may partially function, but the disagg-specific behavior and edge cases are untestable until Public Preview.

| ID | Priority | Description | Suggested Test |
|----|----------|-------------|----------------|
| PT-1 | DEFERRED | Prepare + checkpoint before commit — prepared data snapshot visibility on follower after checkpoint advance | `test_layered_prepared01.py` — implement when prepared txn lands |
| PT-2 | DEFERRED | Multiple in-flight prepares during step_up — edge cases with durable_timestamp assignment | extend `test_layered94.py` |
| PT-3 | DEFERRED | Prepare + drain interaction — code at `conn_layered_ingest.c:286` says "temporary solution, assumes no concurrent commit/rollback of the prepared" | `test_layered_prepared03.py` |
| PT-4 | DEFERRED | Prepared rollback after drain has started | `test_layered_concurrency01.py` |
| PT-5 | DEFERRED | Follower cannot see uncommitted prepared data after checkpoint advance | `test_layered_prepared04.py` |

---

## Priority Summary

| Priority | Count | Items |
|----------|-------|-------|
| DEFERRED | 5 | SD-1 through SD-5 (elegant step-down; implement when PuP lands) |
| DEFERRED | 5 | PT-1 through PT-5 (prepared transactions; implement when PuP lands) |
| DEFERRED | 5 | DRP-1 through DRP-5 (table drop; implement when PuP lands) |
| DEFERRED | 1 | FT-1 (fast truncate; implement when PuP lands) |
| MEDIUM | 4 | RTS-1, RTS-2, RTS-3, REN-1 |
| LOW | 10 | RTS-4, RTS-5, ALT-1, ALT-2, NC-1, SAL-1, CMP-1, IMP-1, BLK-1, BAK-1, REC-1, IDX-1, FT-2 |

**Important:** None of these items should block or influence the priority of gaps in the main supported-feature analysis (`00_synthesis.md`). They are tracked separately so they do not inflate or distort the priority ordering of supported-feature gaps.
