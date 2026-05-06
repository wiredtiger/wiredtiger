# Gap Analysis: Checkpoint, RTS, History Store, and Prepared Transactions

Analysis date: 2026-05-01  
Analyst: Ivan Kochin  
Source analyzed: test/analysis/suite/test_checkpoint*.md, test_rollback_to_stable*.md,
test_hs*.md, test_prepare*.md, test_prepare_hs*.md; src/rollback_to_stable/rts*.c;
src/checkpoint/checkpoint_txn.c; test/analysis/checkpoint/checkpoint.md

---

## Duplicate / Overlapping Cases

### [DUP-1] Checkpoint: Checkpoint cursor HS pairing (timestamped vs non-timestamped)
- **Tests:** test_checkpoint18.py, test_checkpoint19.py
- **Overlap:** Both verify that a checkpoint cursor opened after subsequent checkpoints continues to read the correct historical HS version. The only difference is whether the writes use timestamps. The setup, the threat (subsequent checkpoints advancing the HS), and the assertion (cursor sees its own paired HS state) are identical.
- **Recommendation:** Merge into a single parametrized test (timestamped/non-timestamped scenario dimension). Keep both scenario variants but collapse them into one file.

### [DUP-2] Checkpoint: Inconsistent checkpoint atomicity (non-timestamped vs timestamped)
- **Tests:** test_checkpoint10.py, test_checkpoint11.py
- **Overlap:** Both verify the all-or-nothing visibility guarantee for a transaction committing concurrently with a checkpoint. The mechanism (`timing_stress_for_test=[checkpoint_slow]`, two-table writes, crash restart) is identical. The only distinction is whether `commit_timestamp` is set.
- **Recommendation:** Merge into a single parametrized test with a `use_timestamps` scenario dimension.

### [DUP-3] Checkpoint: Prepared-transaction visibility in checkpoint cursor (two tests with same core scenario)
- **Tests:** test_checkpoint12.py, test_checkpoint20.py
- **Overlap:** Both test that a checkpoint cursor returns the pre-prepared value when `ignore_prepare=true` and `WT_PREPARE_CONFLICT` when `ignore_prepare=false`. test_checkpoint20 also tests the commit_ts vs durable_ts boundary, making it the richer test, but its core scenario duplicates test_checkpoint12.
- **Recommendation:** Retain test_checkpoint20 (more coverage), review whether test_checkpoint12 adds unique API-error paths not exercised by test_checkpoint20 before deleting.

### [DUP-4] Checkpoint: Fast-delete (truncated pages) in checkpoint cursor
- **Tests:** test_checkpoint24.py, test_checkpoint25.py
- **Overlap:** Both verify that fast-deleted pages are correctly visible from a checkpoint cursor. test_checkpoint24 uses non-timestamped transactions, test_checkpoint25 uses timestamped transactions and adds `read_timestamp` boundary checks. The setup and structural threat are the same.
- **Recommendation:** Merge into a single parametrized test; the timestamped scenario should additionally exercise the read_timestamp boundary.

### [DUP-5] RTS: Basic remove rollback (01) and all-keys-removed scenario (06)
- **Tests:** test_rollback_to_stable01.py, test_rollback_to_stable06.py
- **Overlap:** Both verify that RTS removes keys/updates committed after stable. test_rollback_to_stable01 (stable=10, removes at ts=20) and test_rollback_to_stable06 (stable before all commits, all 1,000 rows gone) share identical parametrization axes (key_format, in_memory, prepare, dryrun, workers) and the same structural pattern. test_rollback_to_stable06 simply moves stable before all timestamps, making it a boundary case of test_rollback_to_stable01.
- **Recommendation:** Consider consolidating 01 and 06 into one file with a `stable_boundary` scenario dimension.

### [DUP-6] RTS: HS value restoration vs HS modify restoration
- **Tests:** test_rollback_to_stable02.py, test_rollback_to_stable04.py, test_rollback_to_stable14.py, test_rollback_to_stable23.py
- **Overlap:** All four verify that RTS restores a prior value from the HS after unstable updates are rolled back. The differences are minor: 02 uses plain full updates; 04 adds modifies to the chain; 14 uses a concurrent background checkpoint; 23 uses cursor.search() rather than scan to verify reconstruction. The core "RTS finds value in HS and installs it back" code path is exercised identically in all four.
- **Recommendation:** These can coexist but the scenario matrices are heavily redundant. Consider removing test_rollback_to_stable02 in favor of test_rollback_to_stable04 (strictly a superset) and flagging test_rollback_to_stable23 as a verify-mode variant of test_rollback_to_stable14.

### [DUP-7] RTS: Multiple update versions removed across DS + HS
- **Tests:** test_rollback_to_stable16.py, test_rollback_to_stable17.py
- **Overlap:** test_rollback_to_stable16 tests on-disk update removal using distinct key-range batches; test_rollback_to_stable17 tests the same scenario but with updates spanning both DS and HS simultaneously, using the same 200-row table. Both parametrize on key_format, in_memory, and worker threads. The comment in test_rollback_to_stable16 itself notes it may be redundant with others.
- **Recommendation:** If test_rollback_to_stable17 is kept (it adds the DS+HS split), then test_rollback_to_stable16 can be retired.

### [DUP-8] RTS: HS tombstone restoration (multiple methods in one file)
- **Tests:** test_rollback_to_stable13.py (four methods within one file)
- **Overlap:** `test_rollback_to_stable_with_aborted_updates` is nearly identical to `test_rollback_to_stable` within the same file: it adds rolled-back updates to the chain but the assertion (tombstone at ts=50, value_a at ts=20) and stat check (`hs_restore_tombstones == nrows`) are identical. The aborted updates have no measurable effect on the output.
- **Recommendation:** Either remove `test_rollback_to_stable_with_aborted_updates` or demote it to a comment explaining why aborted updates don't affect the outcome.

### [DUP-9] HS: Non-timestamped tombstone clears HS records (file-level vs table-level)
- **Tests:** test_hs11.py, test_hs31.py, test_hs32.py
- **Overlap:** All three verify that a non-timestamped tombstone/update clears HS records for the affected key. test_hs11 uses the `cache_hs_key_truncate_onpage_removal` stat; test_hs32 uses `cache_hs_key_truncate`; test_hs31 uses a `file:` URI and the `rec_hs_wrapup_next_prev_calls` stat. The parametrization matrices are almost identical (key_format × deletion/update × long-running). test_hs32 adds an optional long-running transaction variant not in test_hs11, but test_hs11 covers 192 scenarios while test_hs32 covers 12.
- **Recommendation:** Determine which stat (`onpage_removal` vs `truncate`) represents which code path and document the distinction clearly. Merge the common scenarios; keep test_hs31 separate because it uses `file:` URI and the wrapup stat.

### [DUP-10] HS: Modify reconstruction after eviction (multiple files)
- **Tests:** test_hs08.py, test_hs10.py, test_hs12.py, test_hs13.py
- **Overlap:** All four verify that modifies stored in HS can be correctly reconstructed after eviction. test_hs08 focuses on squashing; test_hs10 is the simplest case (three sequential modifies, re-read after eviction); test_hs12 tests append/prepend modifies; test_hs13 tests reverse-modify traversal with prepend. The core code path (`__curhs_prev` + forward scan for base + reverse delta application) is exercised in all four.
- **Recommendation:** Retain test_hs08 (squash), test_hs12 (append beyond end), and test_hs13 (reverse traversal) as they test distinct sub-paths. Remove or merge test_hs10 into test_hs08 as it is a subset.

---

## Missing Coverage

### [CRITICAL] Checkpoint: Crash recovery with named checkpoints as recovery point

**What is not tested:**
No test simulates crash+restart where the recovery point is a *named* checkpoint (not `WiredTigerCheckpoint`). All crash-recovery tests use `simulate_crash_restart` which always recovers to the most recent unnamed checkpoint. Named checkpoints are a first-class user feature that MongoDB has used.

**Risk:**
If the named checkpoint path in `wiredtiger_open` recovery diverges from the unnamed-checkpoint path (different metadata traversal, different WAL replay window, different RTS trigger), a corruption or data-loss bug could lurk undetected. Named checkpoints are also the recovery basis when an operator specifies a target.

**Code path analysis:**
- Source: `src/checkpoint/checkpoint_ckptlist.c`, `src/conn/conn_recover.c`
- Branch/condition: `wiredtiger_open` with `checkpoint=(name=<name>)` picks a specific checkpoint from the ckptlist rather than the most recent one. The RTS that runs at startup uses `stable_timestamp` read from that checkpoint's metadata. These are distinct code paths.
- Why tests miss it: All Python crash tests call `simulate_crash_restart()`, which always opens with the default (most recent) checkpoint. There is no parametrization across `checkpoint=(name=...)` in the recovery open.

**Proposed test:**
- Setup: Create 3 named checkpoints (ckpt-A at ts=10, ckpt-B at ts=20, ckpt-C at ts=30). Write additional unstable data above ts=30.
- Operations: Copy the database directory. Reopen the copy with `checkpoint=(name=ckpt-B)`.
- Assertions: Verify data matches what was stable at ckpt-B (ts=20 boundary). Verify RTS runs and removes anything between ts=20 and ckpt-C. Verify the metadata `WiredTigerCheckpoint` pointer now refers to ckpt-B after recovery.

---

### [CRITICAL] RTS: Concurrent RTS + checkpoint + eviction stress test (true three-way concurrency)

**What is not tested:**
No test runs RTS as an explicit concurrent operation while a live checkpoint and aggressive eviction are all running simultaneously. test_rollback_to_stable10 has a background checkpoint thread concurrent with RTS, but RTS is the primary operation and eviction is a side effect. test_rollback_to_stable22 runs RTS + eviction but no concurrent checkpoint. The three-way combination is never exercised.

**Risk:**
RTS takes locks that interact with both checkpoint locks (`schema_lock`, `table_lock`) and eviction locks (`hazard pointers`, `page locks`). A three-way race could produce a deadlock, a livelock, or incorrect page state. This combination occurs in real crash recovery when the server is under heavy write pressure.

**Code path analysis:**
- Source: `src/rollback_to_stable/rts.c` (`__wti_rts_btree_apply_all`), `src/checkpoint/checkpoint_txn.c` (`__checkpoint_prepare`), `src/evict/evict_lru.c`
- Branch/condition: During RTS worker thread dispatch (`rts_push_work`/`rts_thread_run`), eviction threads can concurrently evict pages from the same btree, potentially racing with `__rts_btree_walk`. If a checkpoint is simultaneously in `__checkpoint_prepare` acquiring `schema_lock`, all three operations contend on the same lock hierarchy.
- Why tests miss it: test_rollback_to_stable22 explicitly documents "no checkpoint" in its notes. test_rollback_to_stable10 uses `timing_stress_for_test=[history_store_checkpoint_delay]` but this delays HS checkpoint, not the main checkpoint.

**Proposed test:**
- Setup: 20 tables, 50 MB cache, `cache_size` tight enough to force continuous eviction.
- Operations: Start three concurrent threads: (1) a background checkpoint thread looping every 100ms; (2) an eviction-pressure thread inserting large values to fill cache; (3) the main thread calling `rollback_to_stable()` with decreasing stable_timestamp every iteration.
- Assertions: No panic, no deadlock (timeout-based), all data reads correctly after each RTS call, final stat `txn_rts_calls > 10`.

---

### [CRITICAL] History Store: HS itself is rolled back by RTS (HS final pass coverage)

**What is not tested:**
The `__wti_rts_history_final_pass` function in `src/rollback_to_stable/rts_history.c` performs a dedicated walk of the HS btree itself when `max_durable_ts > rollback_timestamp`. This code path — where the HS itself contains records newer than stable that must be removed — is not explicitly targeted by any Python test. test_rollback_to_stable38 tests HS *truncation* (bulk wipe of a whole btree), not the fine-grained final-pass walk.

**Risk:**
If the HS final-pass walk has a bug (e.g., incorrect stop_ts comparison at line `if (hs_tw->stop_ts <= ts) break;` in `__wti_rts_history_delete_hs`), RTS could leave stale HS records behind that would be read incorrectly on subsequent reads. This is a data correctness bug, not just a performance issue.

**Code path analysis:**
- Source: `src/rollback_to_stable/rts_history.c:__wti_rts_history_final_pass` and `__wti_rts_history_delete_hs`
- Branch/condition: Lines 175–193 in `rts_history.c`: the final-pass walk is only triggered when `S2BT(session)->modified || max_durable_ts > rollback_timestamp`. The `max_durable_ts` is computed from the HS checkpoint metadata's `newest_stop_durable_ts`. If the HS has been modified in a way that its own stop timestamps exceed stable, the walk fires and calls `__wti_rts_history_delete_hs` which iterates backward through HS records.
- Why tests miss it: Most tests roll back the *data store* (which drives HS cleanup via `__wti_rts_history_delete_hs` calls from `__rts_btree_ondisk_fixup_key`). The HS final pass is only needed when the HS itself has records that escaped the per-key cleanup — a rarer scenario requiring very specific timing between eviction, checkpoint, and stable_timestamp advancement.

**Proposed test:**
- Setup: 1,000 rows, 10 MB cache. Write value_a@10, value_b@20. Set stable=20. Force eviction to push HS records. Advance stable to 20.
- Operations: Write value_c@30 to half the rows. Set stable=25. Trigger aggressive eviction that writes the unstable updates through to the HS (not just DS). Run checkpoint. Crash restart.
- Assertions: Verify `txn_rts_hs_removed > 0` specifically from the HS final pass (can be confirmed by verbose RTS logging showing `WT_RTS_VERB_TAG_HS_TREE_ROLLBACK`). Verify all rows read value_b at ts=25.

---

### [HIGH] Checkpoint: Callback ordering and handle acquisition under load

**What is not tested:**
The checkpoint applies to dhandles in the order acquired by `__checkpoint_apply_to_dhandles` (src/checkpoint/checkpoint_txn.c line 328). No Python test verifies that re-running `checkpoint(target=[uri])` on a subset of tables while other tables are being modified produces a consistent checkpoint of *only* the targeted tables. test_checkpoint01's `test_checkpoint_target` exercises a single targeted checkpoint but with no concurrent writers.

**Risk:**
If the handle-acquisition ordering has a race with table creation or drop (particularly during `session.alter` or `session.rename`), a targeted checkpoint could either miss a table or deadlock. MongoDB uses targeted checkpoints during bulk loading.

**Code path analysis:**
- Source: `src/checkpoint/checkpoint_txn.c:__checkpoint_apply_to_dhandles` (line 328), `src/checkpoint/checkpoint_txn.c:__wt_checkpoint_get_handles` (line 377)
- Branch/condition: `__wt_checkpoint_get_handles` walks `conn->dhhash` and locks handles in hash-bucket order. If a concurrent `session.create` adds a new dhandle between two hash-bucket walks, the new table may be included or excluded non-deterministically.
- Why tests miss it: test_checkpoint07 tests the clean-timer reset, test_checkpoint26 tests concurrent eviction, but no test does concurrent `session.create` + targeted checkpoint.

**Proposed test:**
- Setup: 10 existing tables plus a writer thread that continuously creates+drops tables.
- Operations: Run `session.checkpoint(target=[list of 5 stable tables])` repeatedly while create/drop races.
- Assertions: Only the targeted 5 tables appear in the resulting checkpoint (verified via metadata cursor). No EBUSY or corruption.

---

### [HIGH] History Store: Overflow HS records are never tested

**What is not tested:**
The HS stores values as cells in the WiredTigerHS.wt file. When a history store *value* exceeds `leaf_value_max` it should become an overflow item within the HS file itself. No test exercises this path. test_hs20 tests overflow values in the *data store* and their effect on HS reverse-modify reconstruction, but does not produce overflow records inside the HS itself.

**Risk:**
If the HS overflow record write/read path is broken, any key whose historical value exceeds the leaf_value_max configuration would be silently corrupted. Applications using `leaf_value_max` with large values would be affected.

**Code path analysis:**
- Source: `src/history/hs_rec.c` (HS write path) and `src/history/hs_cursor.c` (HS read path)
- Branch/condition: During HS reconciliation (`__wt_hs_insert_updates`), if the accumulated value size exceeds `leaf_value_max` of the HS btree, an overflow cell is written. The HS btree has its own `leaf_value_max` configuration, defaulting to 0 (no limit). If a user sets a small `leaf_value_max` on the connection, HS values could overflow.
- Why tests miss it: HS tests use large values to trigger eviction (e.g., `bigvalue` in test_hs01), but the HS btree itself is never configured with a low `leaf_value_max`. The overflow path in the HS btree is therefore never triggered.

**Proposed test:**
- Setup: Open connection with `leaf_value_max=64` (tiny), write 100 rows with 200-byte values at ts=2, ts=3 (forcing multi-version history into the HS).
- Operations: Checkpoint (forces HS write). Read back at ts=2 and ts=3.
- Assertions: Values match. `stat.conn.cache_hs_insert > 0`. No corruption. Verify via HS checkpoint cursor (`file:WiredTigerHS.wt`) that overflow cells appear (type check).

---

### [HIGH] Checkpoint: Open cursors observing a checkpoint's view (cursor stability during checkpoint evolution)

**What is not tested:**
What happens when a caller holds open a named checkpoint cursor across multiple subsequent checkpoints that overwrite the same named checkpoint? test_checkpoint01's `test_checkpoint_cursor_inuse` verifies that `drop` returns EBUSY, but does not test the scenario where the checkpoint is *overwritten in-place* (a `session.checkpoint("name=foo")` call while `checkpoint=foo` cursor is open). WiredTiger's documented behavior is that EBUSY is returned on overwrite too, but this is not tested.

**Risk:**
If overwriting a named checkpoint while a cursor is open silently succeeds, the cursor's internal page references become stale (pointing to freed pages), leading to a use-after-free. This is a memory safety issue.

**Code path analysis:**
- Source: `src/checkpoint/checkpoint_txn.c:__checkpoint_name_check` (line 227) and `src/session/session_api.c` (cursor open path)
- Branch/condition: `__checkpoint_name_check` should detect that a cursor is open on the named checkpoint before allowing an overwrite. This is the same `WT_BTREE_SPECIAL_FLAGS` check used for drop. If the check is missing for the overwrite case, the cursor sees freed pages.
- Why tests miss it: test_checkpoint01 tests drop-while-open and test_checkpoint13 tests drop-while-open, but neither tests the `session.checkpoint("name=<same name>")` overwrite path with an open cursor.

**Proposed test:**
- Setup: Create named checkpoint `ckpt-A`. Open a checkpoint cursor on `ckpt-A`.
- Operations: Attempt `session.checkpoint("name=ckpt-A")` while the cursor is open.
- Assertions: Returns EBUSY (same as drop). After cursor close, overwrite succeeds. New cursor on `ckpt-A` sees the updated data.

---

### [HIGH] Prepared Transactions: Prepare + RTS + eviction during the prepared window

**What is not tested:**
No test exercises the scenario where a prepared-but-not-committed transaction is evicted while RTS is concurrently running. test_prepare_hs01 loads prepared updates that trigger eviction, but RTS is not called. test_rollback_to_stable10's `test_rollback_to_stable_prepare` checks RTS after prepared txns with concurrent checkpoint, but does not force eviction of the prepared pages during RTS.

**Risk:**
Evicting a page that has a prepared update chain while RTS is walking that same btree could cause RTS to miss the update (page moves from in-memory to on-disk between RTS check and RTS write), leaving a prepared tombstone or value on disk that should have been removed. This is a data correctness threat.

**Code path analysis:**
- Source: `src/rollback_to_stable/rts_btree.c:__rts_btree_abort_update` (line 17) and `src/evict/evict_page.c`
- Branch/condition: `__rts_btree_abort_update` walks the in-memory update chain. If eviction wins the race and writes the prepared update to disk before RTS reaches it, `__rts_btree_abort_ondisk_kv` is responsible for cleaning it up (line 671). The two paths must not both execute or both fail to execute for the same key.
- Why tests miss it: RTS is designed to run exclusively (`WT_CONN_RECOVERING` or `rollback_to_stable()` quiesces eviction). However, during *runtime* RTS (explicit API call), eviction is not stopped. The concurrent eviction + runtime RTS path is not explicitly stress-tested with prepared updates.

**Proposed test:**
- Setup: 10,000 rows, 5 MB cache (tight). Write value_a@10. Prepare value_b@20 in session2 (do not commit). Set stable=10.
- Operations: Start an eviction-pressure thread filling cache with junk. Call `rollback_to_stable()` on the main thread concurrently.
- Assertions: All rows show value_a after RTS. No `WT_PREPARE_CONFLICT` or assertion failure. `txn_rts_upd_aborted > 0`.

---

### [MEDIUM] Checkpoint: Named checkpoint read-timestamp per-checkpoint enforcement under concurrent updates

**What is not tested:**
test_checkpoint15 verifies that each named checkpoint enforces its own `oldest_timestamp` lower bound, but only in a single-threaded setup with no concurrent writes during the read. No test verifies that the per-checkpoint timestamp constraints hold when a new (concurrent) writer advances the global `oldest_timestamp` beyond one of the old named checkpoints' boundaries while a cursor is actively reading from that old checkpoint.

**Risk:**
If the global `oldest_timestamp` advancement affects the per-checkpoint read boundary (because the check uses the global oldest rather than the checkpoint's pinned oldest), reads at timestamps that were valid when the checkpoint was taken could start failing.

**Proposed test:**
- Setup: Named checkpoint `ckpt-1` taken when oldest_ts=5, stable_ts=10. 
- Operations: Open a cursor on `ckpt-1` with `read_timestamp=6`. Advance global `oldest_timestamp` to 15. Attempt reads at `read_timestamp=6` on the already-open cursor.
- Assertions: Reads at ts=6 succeed (checkpoint's own oldest_ts governs, not global). Reads at ts=4 fail (below checkpoint's own oldest).

---

### [MEDIUM] RTS: Multi-table partial backup restore + RTS truncation of orphaned HS entries

**What is not tested:**
The `__wti_rts_history_final_pass` function at lines 200–203 in `rts_history.c` handles a partial backup restore scenario: it truncates HS entries for btrees that no longer exist after selective restore (`WT_CONN_BACKUP_PARTIAL_RESTORE`). No Python test covers this path. The only test for partial backup in this area is test_checkpoint_snapshot02's backup variant, which does not use selective restore.

**Risk:**
Orphaned HS entries from tables dropped during selective restore could prevent new writes at timestamps already present in the HS, or cause incorrect reads if a new table is created with the same btree ID.

**Proposed test:**
- Setup: Two tables. Full backup including both. Run additional transactions on both tables. Selective restore of only one table from backup.
- Operations: Open restored DB. Call `rollback_to_stable()` or trigger recovery RTS.
- Assertions: `cache_hs_btree_truncate > 0` (HS entries for the excluded table were truncated). Reads on the restored table succeed. No stale HS references.

---

### [MEDIUM] History Store: High-concurrency HS write stress under cache pressure

**What is not tested:**
No test simultaneously runs many writer sessions all producing HS writes under cache pressure. test_hs01 is single-threaded. test_rollback_to_stable22 has concurrent updates but focuses on RTS correctness, not HS write throughput or correctness under contention. test_prepare_hs01 uses 3 sessions but the prepared updates themselves don't produce HS writes until eviction.

**Risk:**
The HS is a single shared btree. Under high concurrency, two sessions attempting to insert HS records for the same key at different timestamps must serialize correctly. A missing lock or incorrect counter could produce duplicate HS entries or incorrect ordering.

**Proposed test:**
- Setup: 16 writer sessions, 10,000 rows, 50 MB cache. Each session writes to its own key range but at overlapping timestamps to pressure the HS insert path.
- Operations: All 16 sessions write 5 rounds of updates at increasing timestamps with stable_ts held back (forcing every checkpoint to push to HS). Run 10 checkpoints.
- Assertions: No panic. `cache_hs_insert` equals expected total versions. After final crash-restart, reads at each timestamp return the correct values across all key ranges.

---

### [MEDIUM] Prepared Transactions: Prepared truncation with RTS

**What is not tested:**
test_rollback_to_stable34 tests RTS with fast-truncation (truncation committed past stable), and test_checkpoint28 tests prepared transactions with concurrent checkpoint. But no test combines prepared fast-truncation with RTS. A prepared `session.truncate()` that is evicted to disk and then rolled back by RTS is a distinct code path from either prepared updates or non-prepared truncation.

**Risk:**
RTS's `__rts_btree_walk_page_skip` (line 30–112 in rts_btree_walk.c) has an assertion: `page_del->prepare_state == WT_PREPARE_INIT || page_del->prepare_state == WT_PREPARE_RESOLVED`. The comment says "prepared truncates can't be written to disk." If that invariant is ever violated (e.g., by a bug in the checkpoint of prepared truncates), RTS would encounter an assertion failure or silently skip an unstable truncation.

**Proposed test:**
- Setup: 5,000 rows. Prepare a truncation of rows 1,000–4,000.
- Operations: Attempt to evict the fast-delete refs. Attempt checkpoint with the prepared truncation. Verify the checkpoint does not include the prepared truncation on disk. Roll back the prepared truncation. Verify rows are restored.
- Assertions: Checkpoint cursor does not see the truncated range as deleted. After rollback, all rows visible. `rec_page_delete_fast > 0` only after committed truncation.

---

### [MEDIUM] Checkpoint: Crash recovery test coverage (Python tests largely skip crash)

**What is not tested:**
Of the 37 test_checkpoint*.py tests and 6 test_checkpoint_snapshot*.py tests, only about 10 use `simulate_crash_restart` or `copy_wiredtiger_home`. The majority of checkpoint tests are single-restart or no-restart tests that exercise the checkpoint API but not the crash recovery code path. In particular, tests 16 (clean table readable from checkpoint), 27 (metadata page eviction), 29 (bulk load), 30 (aggregate time window), and 32 (fast-delete stat) all exercise important checkpoint behaviors but none of them validate what happens after crash+recovery.

**Risk:**
A bug that only manifests during crash recovery (e.g., incorrect WAL replay of checkpoint metadata, incorrect RTS after recovery with specific checkpoint configurations) would not be caught by API-only tests.

**Proposed test additions:**
- Add `simulate_crash_restart` variants to test_checkpoint16, test_checkpoint29, and test_checkpoint30.
- For test_checkpoint16 (clean-table optimization): verify that after crash+recovery, the clean table is still readable from the new `WiredTigerCheckpoint`.
- For test_checkpoint30 (aggregate time window): verify that after crash+recovery, the aggregate time window on internal pages is correctly re-built and partial-visibility is preserved.

---

### [LOW] RTS: Incremental btree-skip (`txn_rts_btrees_skipped`) vs btree apply under worker threads

**What is not tested:**
test_rollback_to_stable20 verifies that RTS skips dhandles that have no unstable updates (checking `dh_conn_handle_count < 5`). However, this test uses zero worker threads. With `rts_threads > 0`, the work unit dispatch (`__rts_push_work`) uses a queue; it is possible the skip logic (the `file_skipped` branch in `__wti_rts_btree_walk_btree_apply`) behaves differently under multi-threaded dispatch because `WT_STAT_CONN_INCR(session, txn_rts_btrees_skipped)` is called on the dispatcher thread, not the worker.

**Proposed test:**
Extend test_rollback_to_stable20 to include a worker_thread_values dimension (0, 2, 4 threads). Verify that `txn_rts_btrees_skipped` is the same regardless of thread count.

---

### [LOW] RTS: Interaction between RTS dryrun and subsequent real RTS on HS state

**What is not tested:**
test_rollback_to_stable41 verifies dryrun is per-call (does not persist). But no test verifies that a dryrun RTS call followed immediately by a real RTS call produces the same result as a single real RTS call. Dryrun skips actual removes (`if (!dryrun) WT_ERR(hs_cursor->remove(hs_cursor))` in `rts_history.c` line 62), but it still advances cursor positions and stats counters. If dryrun leaves internal state (cursor positions, ref counts) in a different state from a real call, the subsequent real call could behave unexpectedly.

**Proposed test:**
- Setup: 1,000 rows, multiple versions. Set stable below all commits.
- Operations: Call dryrun RTS. Immediately call real RTS.
- Assertions: Real RTS produces exactly the same stat deltas (`upd_aborted`, `hs_removed`) as if dryrun was not called first. Final data state is identical.

---

## Summary Table

| Priority | Area | Gap | Risk |
|---|---|---|---|
| CRITICAL | Checkpoint | Crash recovery via named checkpoint | Data loss, undetected recovery divergence |
| CRITICAL | RTS | Concurrent RTS + checkpoint + eviction three-way stress | Deadlock, livelock, or incorrect page state |
| CRITICAL | History Store | HS final pass (HS itself rolled back by RTS) | Stale HS records, incorrect reads post-RTS |
| HIGH | Checkpoint | Callback ordering / handle acquisition with concurrent create+drop | Deadlock, missing table in checkpoint |
| HIGH | History Store | Overflow records inside the HS btree | Silent corruption of large historical values |
| HIGH | Checkpoint | Open cursor vs named checkpoint overwrite (EBUSY enforcement) | Use-after-free on stale checkpoint page |
| HIGH | Prepared Txns | Prepare + RTS + concurrent eviction during prepared window | RTS misses prepared update, corruption |
| MEDIUM | Checkpoint | Per-checkpoint oldest_ts enforcement with advancing global oldest | Incorrect read boundary, spurious errors |
| MEDIUM | RTS | Partial backup restore + RTS HS orphan truncation | Orphaned HS entries, incorrect btree-id reuse |
| MEDIUM | History Store | High-concurrency HS write stress | Duplicate HS entries, incorrect ordering |
| MEDIUM | Prepared Txns | Prepared truncation (fast-delete) + RTS | Silent skip of unstable truncation |
| MEDIUM | Checkpoint | Crash recovery tests for checkpoint16, 29, 30 | Recovery bugs undetected without crash path |
| LOW | RTS | Btree-skip correctness under worker thread dispatch | Incorrect `btrees_skipped` stat, possible skipped work |
| LOW | RTS | Dryrun followed by real RTS leaves clean state | Unexpected behavior after dryrun+real sequence |

---

## Duplicate Consolidation Priority Table

| Priority | Tests | Action |
|---|---|---|
| High | test_checkpoint18 + test_checkpoint19 | Merge into parametrized test |
| High | test_checkpoint10 + test_checkpoint11 | Merge into parametrized test |
| High | test_rollback_to_stable02 + test_rollback_to_stable04 | Remove 02, keep 04 as superset |
| High | test_rollback_to_stable16 + test_rollback_to_stable17 | Remove 16, keep 17 |
| Medium | test_checkpoint24 + test_checkpoint25 | Merge into parametrized test |
| Medium | test_checkpoint12 + test_checkpoint20 | Review 12, possibly retire |
| Medium | test_hs11 + test_hs31 + test_hs32 | Unify stat tracking, merge test_hs11 and test_hs32 |
| Medium | test_hs08 + test_hs10 + test_hs12 + test_hs13 | Remove test_hs10 (subset of test_hs08) |
| Low | test_rollback_to_stable01 + test_rollback_to_stable06 | Add boundary scenario to 01, retire 06 |
| Low | test_rollback_to_stable13 within-file | Remove `test_rollback_to_stable_with_aborted_updates` method |
