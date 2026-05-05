# Gap Analysis: Layered Table Checkpoint, Timestamps, and RTS

*Coverage analyzed against: test_layered17, 25, 45, 53, test_disagg_checkpoint_size01-04, test_rollback_to_stable01, test_rollback_to_stable09*

*Source analyzed: src/conn/conn_layered_page_log.c, src/conn/conn_layered_ingest.c, src/conn/conn_layered.c, src/rollback_to_stable/\*, src/btree/bt_handle.c*

*Date: 2026-05-01*

---

## Current Coverage Summary

### Checkpoint timestamp propagation (test_layered17)
Three-phase test: stable at 100, 200, then stable at 250 with data at 300. Verifies that: (a) the checkpoint timestamp in the page log equals the stable timestamp, not the commit timestamp; (b) a follower that picks up the checkpoint sees the correct `last_checkpoint` value; (c) data committed above the stable timestamp is invisible to the follower. Parametrized over three table-type declarations. **Covers:** basic timestamp→checkpoint→follower propagation. **Does not cover:** follower with mismatched local oldest/stable; concurrent stable-timestamp advancement during checkpoint; page-log-write success + metadata-update failure.

### Historical reads after restart (test_layered25)
Two-part restart test: first reopen preserving local files, then `restart_without_local_files`. Both verify that timestamped reads at an older timestamp (ts=100) work after reopen, confirming that history store data is durably stored in the page log. **Covers:** basic HS reconstruction on restart. **Does not cover:** HS reconstruction when the restart stable timestamp is set to a value different from the checkpointed one; partial HS pages missing from the page log.

### Prepared transactions and deltas (test_layered45)
Six sub-cases cover: uncommitted update blocking delta, committed delete + uncommitted guard, update-restore after eviction, prepared update, prepared delete, prepared update+delete. All use `preserve_prepared=true` and `delta_pct=100`. **Covers:** single prepared transaction across one checkpoint boundary; delta skip while prepared is outstanding. **Does not cover:** prepared transaction spanning two checkpoints (prepare ts < checkpoint N, commit ts > checkpoint N+1); multiple simultaneously outstanding prepared transactions on the same page; prepared transaction rolled back after checkpoint N but before drain on step-up; crash between prepare-delta write and metadata-update.

### Stable-timestamp-only checkpoint / follower idempotence (test_layered53)
Verifies that a checkpoint with no new data can advance the stable timestamp, follower picks it up, and re-advancing the same checkpoint is idempotent. **Covers:** clean empty checkpoint; follower duplicate-pickup detection. **Does not cover:** consecutive run of N>2 empty checkpoints with monotonically advancing stable timestamps; checkpoint containing only shared-metadata-table updates with no user-table deltas; oldest_timestamp advancing without data changes between consecutive empty checkpoints.

### Checkpoint size accounting (test_disagg_checkpoint_size01-04)
Covers: non-compressed vs compressed size; monotonic growth; size persistence across restart; failed-checkpoint / crash-restart isolation; multi-btree size aggregation; drop reclaims size; drop one of multiple tables. Also regression tests for three specific bytes-total leak paths (single-page rewrite, delta chain, eviction+reread). **Covers:** all major steady-state and regression size cases. **Does not cover:** concurrent eviction during an in-progress checkpoint; table renamed before next checkpoint; partial drop (table dropped mid-checkpoint before metadata update completes).

### RTS on standard tables (test_rollback_to_stable01, 09)
Generic RTS tests: remove-restore back to stable; schema operations not rolled back. Both are storage-mode-general and explicitly skip tiered. **Neither test runs on disagg/layered tables.** They do not exercise the `WT_BTREE_DISAGGREGATED` flag path in `rts_btree.c:792` or the disagg-specific `WT_UPDATE_DURABLE` mark applied to restored updates.

---

## Duplicate / Overlapping Cases

### test_layered53 vs test_layered17 — follower `last_checkpoint` timestamp
Both tests assert that `conn.query_timestamp('get=last_checkpoint')` on the follower matches the checkpoint timestamp after `disagg_advance_checkpoint`. The assertion is identical in both; one of the two should own this check, or they should be combined.

### test_disagg_checkpoint_size01 vs test_disagg_checkpoint_size02 — size-after-restart
`test_disagg_checkpoint_size01.test_checkpoint_size_persists_across_restart` and `test_disagg_checkpoint_size02.test_database_size_persists_across_restart` both verify that size survives a clean reopen (`lose_all_my_data=true` path). They read from different fields (`size` in stable-file metadata vs `database_size` in the checkpoint completion record) so they are not full duplicates, but the test description and setup are nearly identical. They could be unified with a single helper that validates both fields in one restart pass.

### test_disagg_checkpoint_size03 — two separate bytes-total-leak tests with similar structure
`test_bytes_total_leak` and `test_bytes_total_leak_delta` share the same invariant (final size < 2× baseline after repeated rewrites) and differ only in `delta_pct`. They could be merged as a parametrized scenario to reduce maintenance surface.

---

## Missing Coverage

### [MEDIUM] RTS applied to ingest btrees — behavior test (RTS-1 in `08_unsupported_features.md`)

**Classification: `rollback_to_stable` is NEVER supported on disagg connections.**

RTS is classified as NEVER for disaggregated storage: disagg connections skip recovery RTS globally
(`txn_recover.c:1355-1398` and `txn.c:2593-2594` both short-circuit when `disagg=true`), and
explicit `session.rollback_to_stable()` calls are also unsupported. This is tracked as RTS-1
through RTS-5 in `05_scenario_analysis/08_unsupported_features.md`.

The test goal is a **negative/behavior test**: call `session.rollback_to_stable()` on a disagg
connection and confirm the correct error is returned (or that the call is cleanly skipped without
corrupting data). This is a MEDIUM priority behavior test, not a CRITICAL safety gap.

**What to test (behavior test):**
- Setup: open a disagg leader, insert data at ts=10, checkpoint. Insert more data at ts=20.
  Call `session.rollback_to_stable()` explicitly.
- Assertions: either the call returns an error (expected behavior — RTS is unsupported on disagg),
  or if the call is silently skipped, verify data is not corrupted and `txn_rts_btrees_applied`
  shows zero btrees were walked.

**Code path reference (for implementation context):**
- `src/btree/bt_handle.c:621-623`: ingest btrees carry `WT_BTREE_GARBAGE_COLLECT` but not
  `WT_BTREE_DISAGGREGATED`; the `WT_BTREE_LOGGED` guard in `rts_btree_walk.c:486` does not
  protect them.
- `src/txn/txn_recover.c:1355-1356` and `src/txn/txn.c:2593-2594`: global disagg skip paths
  that must be verified to fire correctly.

---

### [CRITICAL] Prepared transaction spanning two checkpoint boundaries — drain correctness unknown

**What is not tested:**
All `test_layered45` prepared-transaction sub-cases prepare and commit/rollback within the scope of a single checkpoint pair (prepare before checkpoint N, commit after checkpoint N but before step-up). There is no test for a prepared transaction that spans *two* checkpoint boundaries: prepare_ts < checkpoint N < checkpoint N+1, with commit_ts > checkpoint N+1.

**Risk:**
`__layered_copy_ingest_table` (conn_layered_ingest.c:309) reads the `last_checkpoint_timestamp` once at drain start (line 334-335) and uses it as the `start_timestamp` filter for the version cursor (line 341-342). If a prepared transaction was resolved (committed) between checkpoint N and checkpoint N+1, and checkpoint N+1 is the checkpoint at which step-up occurs, the drain will copy data using checkpoint N+1's timestamp. If instead step-up occurs at checkpoint N (before the commit), the drain skips the uncommitted prepared update entirely and the stable btree misses the data. The code at line 404 (`if (prepare || durable_start_ts > last_checkpoint_timestamp)`) only passes data to the stable btree if it is still in prepare state OR was committed after the last checkpoint. This logic has not been exercised with the checkpoint N / checkpoint N+1 boundary straddled by both prepare and commit.

**Code path analysis:**
- `src/conn/conn_layered_ingest.c:334-335`: `last_checkpoint_timestamp` is set to the global `last_checkpoint_timestamp` at drain start.
- `src/conn/conn_layered_ingest.c:404`: condition `prepare || durable_start_ts > last_checkpoint_timestamp` decides whether an ingest update is copied to the stable btree.
- `src/conn/conn_layered_ingest.c:286`: comment: "This is a temporary solution. It assumes no concurrent commit/rollback of the prepared" — indicating the design does not yet account for all prepared-transaction interaction patterns.
- Why existing tests miss it: `test_layered45` always commits or rolls back the prepared transaction within a single checkpoint cycle. No scenario exists where the prepared transaction survives across two separate complete checkpoints before step-up triggers drain.

**Proposed test design:**
- Setup: create layered table, insert baseline data at ts=5, checkpoint (checkpoint 1). Begin transaction, insert at ts=10, prepare at prepare_ts=10 with prepared_id=1. Set stable=10, take checkpoint 2 (prepared update is in delta). Advance stable=20, take checkpoint 3 (prepared update still unresolved). Now commit at commit_ts=15, durable_ts=20. Initiate step-up (drain).
- Assertions: after drain, stable btree contains the committed value. Follower that was advanced to checkpoint 3 (before commit) should not see the value; a fresh step-up from checkpoint 3 should see it after drain.

---

### [HIGH] Crash between page-log metadata write and local metadata update — no recovery test

**What is not tested:**
`__wt_disagg_put_checkpoint_meta` in `conn_layered_page_log.c` (lines 563-672) writes the metadata page to the page log and then, only if successful, updates the in-memory bookkeeping (`last_checkpoint_meta_lsn`, `last_checkpoint_timestamp`, etc.). This is a two-phase commit pattern. There is no test for the failure window: the page log write succeeds (LSN is assigned), but the process crashes before the local metadata table is updated via `__disagg_save_checkpoint_meta_local`. After recovery, the page log has a newer LSN than the local metadata, and a follower could pick up a checkpoint whose LSN exists in the page log but whose local metadata has not been applied.

**Risk:**
After crash-recovery on the leader, the local metadata is at checkpoint N. The page log contains a complete record for checkpoint N+1. A follower that advances to N+1 via `disagg_advance_checkpoint` will apply the shared metadata from N+1, but the leader, after recovery, will try to apply N+1 again (same LSN as what is already in the page log), potentially creating an inconsistency where the follower is one checkpoint ahead of the leader's local view.

**Code path analysis:**
- `src/conn/conn_layered_page_log.c:639`: `__disagg_put_meta(session, WT_DISAGG_METADATA_MAIN_PAGE_ID, metadata_buf, &lsn)` — page log write, can succeed without crashing.
- `src/conn/conn_layered_page_log.c:645-660`: in-memory bookkeeping updated atomically after the write. If crash occurs between line 639 and line 645, the page log has N+1 but in-memory state is still N.
- `src/conn/conn_layered.c:543-557`: `__disagg_pick_up_checkpoint` correctly handles `metadata_lsn == current_meta_lsn` (logs warning, returns). But after crash+recovery, `current_meta_lsn` is restored from metadata, not from the page log — so the leader would see current=N and try to apply N+1, but the local metadata may have been only partially updated if the crash occurred during `__disagg_save_checkpoint_meta_local`.
- No crash injection points exist in the page-log metadata write path (unlike the key rotation path, which has `KEY_PROVIDER_CRASH_BEFORE/DURING/AFTER_KEY_ROTATION` injection points at lines 423, 447, 464).
- Why existing tests miss it: `test_disagg_checkpoint_size02.test_failed_checkpoint_no_size_change` uses `simulate_crash_restart` but only crashes after data inserts without a checkpoint — it does not crash mid-checkpoint after the page-log write but before the metadata-table update.

**Proposed test design:**
- Setup: use a custom page log backend (or the palite test-only backend) that supports a crash injection hook triggerable after `plh_put` returns but before the caller's bookkeeping runs.
- Operations: insert data, begin checkpoint, allow page-log write to succeed, inject crash, restart.
- Assertions: (a) leader re-opens successfully; (b) leader's `query_timestamp('get=last_checkpoint')` reflects checkpoint N (the one before the crash), not N+1; (c) a follower that was at N+1 can still advance (or correctly detects the inconsistency); (d) the leader can take a new checkpoint N+2 successfully.

---

### [MEDIUM] RTS on the stable btree with `WT_UPDATE_DURABLE` — behavior test (RTS-2 in `08_unsupported_features.md`)

**Classification: `rollback_to_stable` is NEVER supported on disagg connections.**

Same classification as the RTS-1 gap above. The `WT_UPDATE_DURABLE` code path in `rts_btree.c:792`
is dead code from the perspective of production disagg workloads — RTS is globally skipped for
disagg connections before any btree-level RTS code is reached. This is tracked as RTS-2 in
`05_scenario_analysis/08_unsupported_features.md`.

The test goal is a **behavior test**: verify the `WT_BTREE_DISAGGREGATED` flag guard in
`rts_btree.c:792` either fires correctly or is not reached due to the earlier global skip.

**What to test (behavior test):**
- The disagg-specific path `if (F_ISSET(S2BT(session), WT_BTREE_DISAGGREGATED)) F_SET(upd, WT_UPDATE_DURABLE)` in `src/rollback_to_stable/rts_btree.c:792-793` should never be reached in a production disagg connection because the global skip fires first.
- Confirm with a test that verifies `txn_rts_btrees_applied` is 0 (or the call returns an error) when RTS is invoked on a disagg connection — regardless of whether `.wt_stable` btrees have data.

**Code path reference:**
- `src/rollback_to_stable/rts_btree.c:792-793`: disagg-specific `WT_UPDATE_DURABLE` mark on restored updates — only reachable if global skip fails.
- `src/txn/txn.c:2593-2594`: global disagg skip that should prevent this path from being reached.

---

### [HIGH] Follower checkpoint pickup with local oldest_timestamp older than checkpoint's oldest — no validation test

**What is not tested:**
When `__disagg_pick_up_checkpoint` processes a new checkpoint, it stores the checkpoint's `oldest_timestamp` from the shared metadata into `last_checkpoint_oldest_timestamp` (conn_layered.c:490), but it does NOT call `set_timestamp` to advance the follower's local `oldest_timestamp` or `stable_timestamp` to match the checkpoint. No test verifies what happens when the follower's locally set `oldest_timestamp` is older than the checkpoint's, or when the follower's stable is older than the checkpoint's stable.

**Risk:**
A follower that has its `oldest_timestamp` set lower than the checkpoint's will retain history store entries that the leader already declared obsolete. If the follower then performs a read with an `as_of` timestamp between its (older) local oldest and the checkpoint's oldest, it may return data that the leader has already garbage-collected. Conversely, if the follower's stable is set higher than the checkpoint's stable and the follower is promoted to leader, the drain may see a mismatch between `last_checkpoint_timestamp` and the local stable, causing incorrect filtering at conn_layered_ingest.c:404.

**Code path analysis:**
- `src/conn/conn_layered.c:469-511`: `__disagg_finalize_checkpoint_meta` stores `metadata->checkpoint_timestamp` and `metadata->oldest_timestamp` in atomic globals but does NOT call `__wt_txn_set_timestamp` or any API that would advance the follower's transaction-global timestamps.
- `src/conn/conn_layered.c:491`: only `txn_global.last_ckpt_timestamp` is updated directly; the follower's `txn_global.oldest_timestamp` and `txn_global.stable_timestamp` are left unchanged.
- No existing test checks whether `follower.query_timestamp('get=oldest')` advances after checkpoint pickup, or validates that the follower correctly refuses to serve reads with timestamps below the checkpoint's oldest.
- Why existing tests miss it: test_layered17 and test_layered53 only check `get=last_checkpoint`; they do not set the follower's timestamps independently and verify conflict detection.

**Proposed test design:**
- Setup: leader inserts at ts=10, sets oldest=5, stable=10, checkpoints. Follower picks up checkpoint. Follower locally sets `oldest_timestamp=1` (older than checkpoint's oldest=5). Attempt timestamped read at ts=3 on the follower.
- Assertions: either the read is rejected (correct behavior — ts=3 is below the checkpoint's oldest), or if it succeeds, verify the returned value is correct and matches what the leader would return. Also test the reverse: follower's stable is set to 15 (above checkpoint stable=10) before step-up; after drain, confirm stable btree sees data consistent with checkpoint stable=10.

---

### [HIGH] Concurrent eviction during checkpoint — size accounting not tested

**What is not tested:**
`test_disagg_checkpoint_size03` tests the bytes-total leak after sequential evict-then-checkpoint cycles. No test exercises the scenario where eviction is happening concurrently with an in-progress checkpoint, specifically the case where a page is evicted (and written to the page log) while the checkpoint is simultaneously reconciling the same btree. The `cumulative_size` accounting in `block_disagg_read.c` is only corrected when a page is read back after eviction — but if eviction happens mid-checkpoint, the checkpoint may write the old block reference while eviction writes a new one.

**Risk:**
Size undercount or overcount depending on timing. The FIXME in test_disagg_checkpoint_size03 (`test_cumulative_size_leak_after_eviction`) notes the specific bug, but only tests it in a strictly sequential (not concurrent) manner. A concurrent eviction during a live checkpoint could expose a different manifestation: the checkpoint sees the pre-eviction `cumulative_size` while the block layer has already freed the old block, causing a double-free or leaked bytes-total.

**Code path analysis:**
- `src/block_disagg/block_disagg_read.c` (noted in test analysis): `cumulative_size` is set to only the most recent delta's raw size on read-in; this is now fixed for sequential cases.
- The fix path is in `rec_write.c` (`disagg_page_free_required` flag) and `__wt_ref_block_free`. If eviction races with checkpoint reconciliation of the same page, the checkpoint's `__wt_ref_block_free` may free a block that was already superseded by the concurrent eviction, resulting in a double-free of the page log entry and incorrect `bytes_total`.
- Why existing tests miss it: all checkpoint-size tests use sequential workloads; no test uses concurrent threads that evict pages simultaneously with a running checkpoint.

**Proposed test design:**
- Setup: layered table with `delta_pct=90`, 100 rows. Start a background thread that continuously reads and force-evicts pages via a debug cursor. Concurrently run 10 checkpoints from the main thread.
- Assertions: after all checkpoints, `database_size` in the completion record is within a reasonable bound (not 2× or more of the actual data size). Stat `rec_page_delta_leaf` is non-zero (confirming the delta path was exercised). No assertion error is hit (no double-free panic).

---

### [MEDIUM] Multiple consecutive empty checkpoints advancing oldest_timestamp — ingest GC prune behavior

**What is not tested:**
`test_layered53` tests two consecutive empty checkpoints (one with no new data, one from the follower that is rejected). No test exercises a sequence of N > 3 empty checkpoints on the leader, each advancing both `stable_timestamp` and `oldest_timestamp`, and verifies that the ingest table's `prune_timestamp` is correctly advanced at each step, enabling GC to reclaim ingest-table pages at the correct granularity.

**Risk:**
`__wti_layered_iterate_ingest_tables_for_gc_pruning` (conn_layered.c:502-505) is called during `__disagg_finalize_checkpoint_meta` with `metadata->checkpoint_timestamp`. If `oldest_timestamp` advances faster than the prune timestamp is updated, ingest pages with timestamps between the old prune and the new oldest may be incorrectly retained or prematurely evicted. This is particularly subtle for the follower, where prune timestamp is updated by picking up checkpoints rather than by running them locally.

**Code path analysis:**
- `src/conn/conn_layered.c:503-504`: `__wti_layered_iterate_ingest_tables_for_gc_pruning(session, metadata->checkpoint_timestamp)` — called with checkpoint_timestamp, not oldest_timestamp.
- `src/conn/conn_layered_ingest.c:728-850`: `__layered_update_ingest_table_prune_timestamp` computes the prune timestamp from the checkpoint order on the stable btree; if no new data was written (empty checkpoint), the stable btree's last checkpoint may not advance, causing the prune timestamp to stall.
- Why existing tests miss it: test_layered53 only performs two empty checkpoints and does not verify the prune timestamp or GC behavior; no test traces the prune timestamp through multiple empty checkpoint rounds.

**Proposed test design:**
- Setup: layered table, insert 100 rows at ts=10, checkpoint. For N=5 iterations: advance oldest and stable by +10, run empty checkpoint (no new data), verify `prune_timestamp` on the ingest btree advanced.
- Assertions: after N iterations, the ingest btree's `prune_timestamp` equals the latest checkpoint timestamp. GC can evict all ingest pages with timestamps <= prune_timestamp without error. Follower that picks up all N checkpoints also shows the correct prune timestamp progression.

---

### [MEDIUM] Checkpoint with only shared-metadata-table updates and no user-table deltas

**What is not tested:**
When a new layered table is created but no data is written (table creation updates the shared metadata table, `WiredTigerShared.wt_stable`), a checkpoint will produce a new shared metadata entry but zero user-table deltas. No test verifies that this specific checkpoint type (metadata-only) is correctly reflected in the completion record's `database_size`, is correctly propagated to a follower, and that a subsequent `restart_without_local_files` can reconstruct the empty table from the page log.

**Code path analysis:**
- `src/conn/conn_layered_page_log.c:601-613`: formats metadata including `largest_file_id`, which advances when tables are created. A table-create checkpoint will advance `largest_file_id` without any user-data pages being written.
- `src/conn/conn_layered.c:461-465`: `__raise_next_file_id` uses `metadata->largest_file_id` to advance `conn->next_file_id` on follower pickup. If `largest_file_id` is not correctly stored or retrieved, the follower will fail to open the new table after restart.

**Proposed test design:**
- Setup: leader creates 5 layered tables sequentially, checkpointing after each creation (no data inserted). Follower picks up each checkpoint.
- Assertions: (a) `database_size` in completion record is non-zero and grows with each table-create checkpoint; (b) after `restart_without_local_files`, the leader can open all 5 tables; (c) `largest_file_id` in the page-log metadata matches the expected value after all 5 creates.

---

### [MEDIUM] Prepared transaction rolled back during drain at step-up — state consistency

**What is not tested:**
`test_layered45` tests that a prepared transaction rolled back *before* step-up is handled correctly in the delta. No test exercises the scenario where a prepared transaction is still in-flight at the moment `__wti_layered_drain_ingest_tables` runs: the drain iterates the ingest btree and encounters a still-prepared (non-committed, non-aborted) update whose `prepare_state == WT_PREPARE_INPROGRESS`.

**Risk:**
The drain comment at conn_layered_ingest.c:286 states: "This is a temporary solution. It assumes no concurrent commit/rollback of the prepared" — explicitly acknowledging that concurrent resolution is not safe. During step-up, however, it is possible that the application has an open session with a prepared transaction. The `__layered_fix_prepared_transaction_callback` in conn_layered_ingest.c walks all sessions and patches WT_TXN_OP entries to point to the stable btree instead of the ingest btree — but this walk has a race window: between the session walk and the subsequent `__layered_copy_ingest_table` call, the application could commit or rollback the prepared transaction.

**Code path analysis:**
- `src/conn/conn_layered_ingest.c:210-300`: `__layered_fix_prepared_transaction_callback` and `__layered_fix_prepared_transaction` walk sessions to redirect WT_TXN_OP entries.
- The walk is not atomic with the drain — no global lock prevents the application from committing/rolling back the prepared transaction during the drain window.
- The `FIXME-WT-14734` note (line 636) about the `layered_table_lock` not being held long enough is a related concurrency concern.

**Proposed test design:**
- Setup: layered table, prepared transaction on session A with prepare_ts=10. Simultaneously initiate step-up (drain) and commit the prepared transaction from another thread/session. Use a synchronization primitive (e.g., an event or a post-prepare hook) to sequence the commit in the middle of the drain walk.
- Assertions: after step-up completes, the stable btree contains the committed value (or correctly reflects the rollback). No assertion error or inconsistency is observed. The WT_TXN_OP entries are correctly redirected in all cases.

---

### [LOW] Table drop during an in-progress checkpoint — size accounting and metadata consistency

**What is not tested:**
`test_disagg_checkpoint_size04` tests that a completed drop (followed by a complete checkpoint) reclaims space. No test drops a table while a checkpoint is in progress (i.e., the drop is submitted during the metadata sweep phase of checkpoint). In WiredTiger, drops are queued and applied at the next checkpoint — but if the checkpoint is already in flight, the interaction depends on when the drop-metadata entry is visible to the checkpoint's metadata cursor.

**Code path analysis:**
- `test_disagg_checkpoint_size04` notes: "The drop is explicitly described as queued — it takes effect at the next checkpoint". No test has a concurrent drop+checkpoint.
- Size accounting in `block_disagg` could temporarily double-count if the checkpoint writes the size before the drop is processed, then the drop processes and frees blocks that were already included in the completion record.

**Proposed test design:**
- Setup: create two layered tables, insert 1000 rows each, checkpoint (baseline). Start a checkpoint (using a hook or debug config to pause mid-checkpoint). Simultaneously drop one table. Resume and complete the checkpoint.
- Assertions: `database_size` after the checkpoint is between the single-table size and the two-table size (the drop may or may not have been processed, depending on timing), but must not exceed 2× the two-table baseline.

---

### [LOW] Repeated empty checkpoint idempotence beyond two iterations (test_layered53 extension)

**What is not tested:**
`test_layered53` tests re-advancing the same checkpoint once (idempotent case). No test exercises N=10 or N=100 consecutive empty checkpoints, verifying that the page log does not accumulate stale LSN entries, that the `last_checkpoint_meta_lsn` remains stable, and that the follower does not drift in its `last_checkpoint` timestamp value.

**Proposed test design:**
- Run 20 consecutive empty checkpoints (stable advancing by 1 each time, no data). After each checkpoint, advance the follower. Assert: (a) `meta_lsn` advances by exactly 1 per empty checkpoint; (b) follower's `last_checkpoint` always matches leader; (c) no memory leak in the metadata buffer (check connection stats for unusual growth).

---

## Summary Table

| Priority | Gap | Risk |
|---|---|---|
| MEDIUM | RTS applied to ingest btrees — behavior test (RTS-1; RTS is NEVER on disagg) | Verify global skip fires; confirm ingest btrees are not walked when RTS is called on disagg |
| CRITICAL | Prepared transaction spanning two checkpoint boundaries | Drain filtering on `last_checkpoint_timestamp` may miss a prepared transaction that was committed between checkpoint N and N+1 |
| HIGH | Crash between page-log metadata write and local metadata update | Leader recovery may diverge from follower's view; inconsistent LSN between page log and local metadata |
| MEDIUM | RTS on stable btree `WT_UPDATE_DURABLE` flag — behavior test (RTS-2; RTS is NEVER on disagg) | Verify disagg-specific path in `rts_btree.c:792` is not reached due to global skip |
| HIGH | Follower checkpoint pickup with mismatched local oldest/stable timestamps | Follower may serve reads below the checkpoint's oldest, or step-up may produce incorrect drain behavior |
| HIGH | Concurrent eviction during checkpoint — size accounting race | `bytes_total` may be double-counted or incorrectly freed when eviction races with checkpoint reconciliation |
| MEDIUM | Multiple consecutive empty checkpoints — ingest GC prune timestamp stall | Prune timestamp may stall on empty checkpoints, causing ingest GC to retain pages that should be evicted |
| MEDIUM | Checkpoint with only shared-metadata-table updates (no user data) | `largest_file_id` propagation and `database_size` accuracy for table-create-only checkpoints |
| MEDIUM | Prepared transaction rolled back/committed concurrently during drain | Race window in `__layered_fix_prepared_transaction_callback` between session walk and drain; FIXME-WT-14734 related |
| LOW | Table drop during in-progress checkpoint — size accounting mid-flight | `database_size` may transiently over- or under-count during concurrent drop+checkpoint |
| LOW | Repeated empty checkpoint idempotence beyond 2 iterations | No test for N>2 consecutive empty checkpoints; potential LSN drift or memory growth |
