# Schema and Session Operations: Scenario Gap Analysis

## Coverage Summary

| Operation | Coverage Level | Dedicated Tests |
|-----------|---------------|-----------------|
| `session.create()` | Good — many key_format/value_format combos | ~90 tests create tables |
| `session.drop()` | Minimal — happy-path only | ~5–8 tests |
| `session.truncate()` | Moderate — cursor-range form only | test_layered_fast_truncate01-03, test_layered49, test_layered80 |
| `session.verify()` | Minimal — post-checkpoint only | test_layered32, test_layered52, test_layered54, test_layered63, test_layered66, test_layered74 |
| `session.alter()` | **NONE** | 0 dedicated tests |
| `session.rename()` | **NONE** | 0 dedicated tests |
| `session.salvage()` | Disabled (FIXME-WT-14740) | 0 tests |
| `session.compact()` | Disabled (skipped by hook) | 0 tests |
| `session.import()` | Disabled | 0 tests |

**Hook-imposed exclusions:** key_format=r (RECNO), colgroups, index:, log=(enabled), import=(enabled), bulk cursors (all skipped by hook_disagg.py).

---

## Gap Analysis

### session.create()

**Covered:** key_format=S and key_format=i; value_format=S; standard row-store configuration; tables created as both leader and follower; create + immediate writes.

**Gap 1 [HIGH]: Create during active drain (step-up in progress)**
- Scenario: One layered table is being drained (step_up running drain loop). A second `session.create()` is called for a new layered table.
- Risk: The drain reads `manager->entries` after releasing the lock early (FIXME-WT-14734). Adding a new table during drain could corrupt the entry array.
- Suggested test: test_layered_create_concurrent_drain.py

**Gap 2 [HIGH]: Create + immediate step_down before any writes**
- Scenario: Leader creates a table, immediately steps down before any data is written or checkpointed. What state is the stable btree in?
- Suggested test: Extend test_layered60.py (already tests empty table creation during checkpoint)

**Gap 3 [DEFERRED]: Create-drop-recreate with same URI**
- Scenario: Create `layered:test`, write data, drop it, create `layered:test` again. Are all metadata entries cleaned up? Does the new table start empty?
- Suggested test: test_layered_schema_cycle.py
*(Depends on session.drop(); see DRP-4 in 08_unsupported_features.md)*

**Gap 4 [MEDIUM]: Multiple tables in a single test (mixed key formats)**
- Scenario: Create `layered:a` (key_format=i) and `layered:b` (key_format=S) in the same test. Write to both, checkpoint, verify both.
- Suggested test: Extend test_layered_cursor01.py or new multi-table test

**Gap 5 [LOW]: key_format=r (RECNO)**
- Blocked by hook_disagg.py line 271 (`key_format=r` creates as non-layered). Whether RECNO is supported or not should be documented and a negative test added.

---

### session.drop()

**Note (May 2026):** `session.drop()` is not currently supported in disaggregated storage (targeted for Public Preview, WT-14503). See `08_unsupported_features.md` (DRP-1 through DRP-5) for the revised test goals.

**Covered:** Drop after basic writes+checkpoint; drop of empty table.

**Gap 1 [DEFERRED]: Drop while drain is in flight**
- Scenario: step_up starts, drain is running for table X, `session.drop('layered:X')` is called concurrently.
- Risk: drain holds a `pinned_dhandle` reference but the entry itself could be freed.
- Suggested test: test_layered_drop_concurrent_drain.py

**Gap 2 [DEFERRED]: Drop table with only ingest data (never checkpointed)**
- Scenario: Leader creates table, writes data, never checkpoints. Drop the table. Are the ingest btree pages cleaned up? Is the metadata consistent?
- Suggested test: test_layered_drop_unflushed.py

**Gap 3 [DEFERRED]: Drop with an active follower cursor on the table**
- Scenario: Follower has an open cursor on `layered:X`. Leader drops `layered:X`. What does the follower cursor see on next operation?
- Suggested test: test_layered_drop_active_follower.py

**Gap 4 [DEFERRED]: Drop + immediate re-create with same URI**
- Scenario: Create, write, drop, create again. Verify no leftover metadata; new table starts empty.
- Suggested test: test_layered_schema_cycle.py

**Gap 5 [DEFERRED]: Drop table with active transactions**
- Scenario: A transaction has the table's dhandle pinned. Drop is called. Should block or return EBUSY.
- Suggested test: Extend drop tests with an open txn

---

### session.truncate()

**Covered:** Cursor-range truncate (start/stop cursor form) on follower; fast-truncate scenarios (test_layered_fast_truncate01-03); truncate + search + verify data gone.

**Note (May 2026):** *Fast* truncate (page-range marking optimization) is NOT currently supported in disagg (targeted for Public Preview). *Slow* truncate IS supported (Private Preview). The existing `test_layered_fast_truncate01-03` tests exercise the fast-truncate path on layered tables — those tests cover an unsupported feature. The gaps below are about slow-truncate scenarios that are testable today. See `08_unsupported_features.md` (FT-1, FT-2) for the fast-truncate test goals.

**Gap 1 [HIGH]: Table-URI truncate (non-cursor form)**
- Scenario: `session.truncate('layered:test', None, None, None)` — truncate the entire table.
- All existing tests use the cursor-range form. The table-URI form exercises a different code path and has zero coverage.
- Suggested test: test_layered_truncate_full_table.py

**Gap 2 [HIGH]: Truncate on leader**
- All fast_truncate tests run on a follower. Truncate on a leader (where writes go to ingest) is untested.
- Scenario: Leader writes 1000 keys, truncates, verifies table is empty, checkpoints.
- Suggested test: test_layered_truncate_leader.py

**Gap 3 [HIGH]: Truncate of stable-only data**
- Scenario: Leader writes + checkpoints (data in stable). Truncate. Verify tombstones cover the stable data correctly.
- Existing tests only truncate data that was in ingest at the time.
- Suggested test: test_layered_truncate_stable.py

**Gap 4 [MEDIUM]: Truncate while drain is in flight**
- Scenario: Drain is copying ingest → stable for table X. Truncate(X) is called on the stable side.
- Risk: Drain and truncate may race on stable btree state.
- Suggested test: test_layered_truncate_concurrent_drain.py

**Gap 5 [MEDIUM]: Cursor-range truncate with unbounded start or stop**
- Scenario: `session.truncate(uri, start_cursor, None, None)` or `(uri, None, stop_cursor, None)` — one-sided range.
- Existing tests always provide both endpoints.
- Suggested test: Extend test_layered_fast_truncate02.py

**Gap 6 [MEDIUM]: Truncate + re-insert same keys**
- Scenario: Truncate range [10,50]. Re-insert keys 10-50. Verify no tombstone/stale data interference.
- Suggested test: Extend test_layered_fast_truncate01.py

---

### session.verify()

**Covered:** verify() called after happy-path operations (writes + checkpoint). No failures, no role transitions, no edge states.

**Gap 1 [HIGH]: verify() after role transition (step_down)**
- Scenario: Leader writes and checkpoints. Connection steps down. verify() called while stable btrees are READONLY.
- Does verify() correctly handle READONLY btrees? Is it blocked or runs normally?
- Suggested test: test_layered_verify_after_stepdown.py

**Gap 2 [HIGH]: verify() on table with only ingest data (pre-checkpoint)**
- Scenario: Leader creates table, writes 100 keys, does NOT checkpoint. verify() on the table.
- Risk: Stable btree is empty or non-existent. Verify must handle this gracefully.
- Suggested test: test_layered_verify_ingest_only.py

**Gap 3 [MEDIUM]: verify() with dump_address or other config options**
- Scenario: `session.verify(uri, 'dump_address')` — more thorough verification mode.
- No layered test uses any verify() config options; all use empty config.
- Suggested test: Extend any existing verify test with config options

**Gap 4 [MEDIUM]: verify() after partial drain**
- Scenario: Drain starts (copy phase completes, truncate fails). verify() called on partially-drained table.
- Suggested test: test_layered_verify_partial_drain.py

**Gap 5 [MEDIUM]: verify() on both constituents independently**
- Scenario: verify() on `file:X.wt_stable` and `file:X.wt_ingest` directly (bypassing the layered URI).
- May expose constituent-level corruption not visible through the layered interface.
- Suggested test: Extend test_layered66.py

---

### session.alter()

**Note (May 2026):** `session.alter()` is not supported in disaggregated storage ("No plan"). See `08_unsupported_features.md` (ALT-1).

---

### session.rename()

**Note (May 2026):** `session.rename()` does not exist as a `WT_SESSION` API in this WiredTiger codebase (`schema_rename.c` does not exist; verified via source inspection). Rename of layered tables is an unimplemented capability. The gap analysis below is revised to reflect this.

**Gap 1 [MEDIUM]: Investigate and document rename capability**
- Scenario: Does WiredTiger support table rename at all (for any URI type)? If so, is it expected to work on layered tables? If not, is this intentional and documented?
- Suggested action: Review commit history and WT documentation; file a Jira ticket to either implement or explicitly document as unsupported.

---

### Disabled Operations: Negative Test Coverage

All disabled operations lack even a negative test confirming the correct error is returned.

**Gap 1 [MEDIUM]: salvage() returns correct error**
- hook_disagg.py skip says "not yet implemented" (FIXME-WT-14740). No test confirms what error code is returned.
- Suggested test: test_layered_salvage_negative.py — call salvage, assert WT_ERROR or ENOTSUP

**Gap 2 [MEDIUM]: compact() behavior**
- hook_disagg.py skips test_compact* tests. Is compact a no-op, unsupported, or silently ignored?
- Suggested test: test_layered_compact_behavior.py — call compact, assert behavior is documented and consistent

**Gap 3 [MEDIUM]: import() returns correct error**
- `session.create(uri, 'import=(enabled=true,...)')` is skipped. No negative test.
- Suggested test: test_layered_import_negative.py

**Gap 4 [LOW]: bulk cursor returns correct error**
- `open_cursor(uri, 'bulk')` is skipped. No negative test.
- Suggested test: Add assertion to any test that bulk returns ENOTSUP or similar

**Gap 5 [LOW]: index creation returns correct error**
- `session.create('index:layered_table:idx', ...)` is skipped. No negative test.
- Suggested test: test_layered_index_negative.py

---

## Priority-Ranked Gap List

### CRITICAL

_(No currently actionable CRITICAL items — drop gaps are DEFERRED pending WT-14503; see `08_unsupported_features.md`.)_

### HIGH

1. Truncate table-URI (full table form, slow truncate) → test_layered_truncate_full_table.py
2. Truncate on leader → test_layered_truncate_leader.py
3. Truncate of stable-only data → test_layered_truncate_stable.py
4. verify() after step_down → test_layered_verify_after_stepdown.py
5. verify() on ingest-only table → test_layered_verify_ingest_only.py
6. Create during active drain → test_layered_create_concurrent_drain.py

### MEDIUM

7. Create + immediate step_down → extend test_layered60.py
8. Truncate while drain in flight → test_layered_truncate_concurrent_drain.py
9. Truncate unbounded range (one-sided) → test_layered_truncate_unbounded.py
10. Truncate + re-insert same keys → test_layered_truncate_reinsert.py
11. verify() with config options → extend test_layered66.py

### LOW

12. Mixed key formats in same test → extend test_layered_cursor01.py

### Unsupported (see 08_unsupported_features.md)

`session.drop()` — all drop gaps (DRP-1 through DRP-5) are DEFERRED pending WT-14503.  
Fast truncate gaps (FT-1) are DEFERRED pending Public Preview.  
`session.alter()`, `session.salvage()`, `session.compact()`, `session.import()`, bulk cursors, `key_format=r`, index creation — see `08_unsupported_features.md`.
