# Gap Analysis: Layered Table Role Transitions and Follower Behavior

*Coverage analyzed against: test_layered07, 15, 21, 26, 27, 29, 31, 32, 36, 37, 46, 50, 53, 57*

---

## Current Coverage Summary

The existing test suite provides reasonable coverage of the happy path for role transitions and follower reads:

| Area | Tests | Coverage level |
|---|---|---|
| Leader→follower→leader round-trip | 07, 21, 26 | Good (happy path) |
| Cold restart / no local files | 15, 36, 46 | Good (single restart) |
| Follower checkpoint pickup visibility | 26, 31, 53 | Good |
| Follower cursor stability across pickups | 31, 37 | Good |
| Follower eviction (stable-btree pages) | 50 | Good |
| Follower eviction (dirty-page guard) | 57 | Good |
| Stable-timestamp-only checkpoint | 53 | Good |
| Ingest drain on promotion | 27 | Good |
| Table creation at scale | 29 | Long-test only |
| Step-up error path | None | **Missing** |
| Multiple consecutive role transitions | None | **Missing** |
| Partially written checkpoint on cold restart | None | **Missing** |
| Stable-table creation failure mid-loop | None | **Missing** |
| Open cursors / active transactions during promotion | Partial (31 part 6 disabled) | **Mostly missing** |
| Follower with zero pickups, then table access | None | **Missing** |
| Checkpoint pickup TOCTOU race | None | **Missing** |
| Table drop-and-recreate state in table manager | None | **Missing** |

---

## Duplicate / Overlapping Cases

### test_layered07 vs test_layered26 — Role swap + data visibility

Both tests exercise the leader→follower→leader round-trip and verify that the follower sees data written by the original leader after a checkpoint pickup. The differences are minor:

- **test_layered07** uses two separate `wiredtiger_open()` processes (true two-process) and asserts final record counts.
- **test_layered26** uses `disagg_advance_checkpoint()` explicitly and also checks that the follower sees **zero** records before the pickup.

The core coverage (write, checkpoint, swap, verify) is duplicated. **The before-pickup check in test_layered26 is distinct and valuable; the after-pickup verification in test_layered07 does not add coverage not already in test_layered26.**

Recommendation: Retain both for the two-process vs. single-process distinction, but document that the negative check in test_layered26 is the only additional coverage test_layered07 does not provide.

### test_layered15 vs test_layered36 — Cold restart re-creating stable tables

Both tests call `restart_without_local_files()` and verify that tables are accessible after picking up a checkpoint. test_layered36 is a subset of test_layered15: it tests only layered tables and only one restart, while test_layered15 tests multiple URI types and two restarts. **test_layered36 provides no coverage not already in test_layered15**, except for its parametrization over URI prefix styles (`layered:` vs `table:`+disagg).

Recommendation: test_layered36 can be merged into test_layered15's parametrization. If kept separate, note that its only unique value is testing an empty table (zero records) after cold restart, which test_layered15 does not explicitly cover.

### test_layered21 vs test_layered07 — Follower insert after checkpoint pickup

test_layered21 (follower-start scenario) inserts on the follower and verifies the data without any role transition. test_layered07 also inserts on what becomes the follower (after step-down) and verifies. The exact behavior being exercised — writing to the ingest table while in follower role — is essentially the same code path. Only the trigger differs (initial role vs post-transition role).

---

## Missing Coverage

### [CRITICAL] Gap 1: Step-up failure leaves node in leader role with corrupted state

**What is not tested:**
There is no test that exercises the error path when `__disagg_step_up()` fails partway through. Specifically: the `leader` flag is set to `true` at line 1285 of `conn_layered.c` *before* `__disagg_restart_checkpoint()`, `__layered_create_missing_stable_tables()`, and `__wti_layered_drain_ingest_tables()` are called. If any of these subsequent operations fail, the node is left with `conn->layered_table_manager.leader = true` but without a valid checkpoint epoch or fully drained ingest tables.

**Risk:**
The code at line 1591–1592 handles this by calling `__wt_panic()`:

```c
if (ret != 0 && reconfig && !was_leader && leader)
    return (__wt_panic(session, ret, "failed to step-up as primary"));
```

This means a failed step-up panics the connection. No test verifies: (a) that the panic is triggered at all, (b) what state the connection is in before the panic is triggered, or (c) that a subsequent cold restart recovers correctly from this scenario. If `__wt_panic()` is bypassed in a production scenario (e.g., the step-up is attempted at connection-open time, not reconfig time), the node could operate as leader with an incomplete checkpoint epoch.

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_step_up()` lines 1279–1312 — sets `leader=true` at line 1285, then calls operations that can fail (lines 1293, 1302, 1306). The `WT_CONN_RECONFIGURING_STEP_UP` flag is cleared in the `err:` block at line 1311, but the `leader` flag is **not rolled back**.
- `src/conn/conn_layered.c:__wti_disagg_conn_config()` lines 1461–1467 — calls `__disagg_step_up()` and on failure at line 1591 panics. No rollback of `leader` occurs between the step-up assignment and the panic.
- Triggered when: `__disagg_restart_checkpoint()` returns an error (e.g., PALI layer returns error on `pl_abandon_checkpoint`), or `__layered_create_missing_stable_tables()` fails (e.g., schema error, disk full), or `__wti_layered_drain_ingest_tables()` fails.
- Why current tests miss it: No test injects a fault during step-up. All step-up calls in the test suite use the palite backend which does not inject errors in these paths.

**Proposed test design:**
- Setup: Open a connection as follower with 3–5 populated layered tables. Inject a fault into the PALI layer's `pl_abandon_checkpoint` or intercept `__layered_create_missing_stable_tables` to return an error (use a custom page log extension or fault injection hook).
- Operations: Call `conn.reconfigure('disaggregated=(role="leader")')` and expect it to raise an error or panic.
- Assertions: (1) The reconfigure call returns an error, not success; (2) the connection is in a panic state (subsequent operations return `WT_PANIC`); (3) after re-opening the database from cold, data is consistent.

---

### [CRITICAL] Gap 2: Multiple consecutive role transitions — residual state

**What is not tested:**
No test exercises the pattern leader → follower → leader → follower (two or more complete round-trips) and verifies that the second transition is free of stale state from the first. Specifically, after step-down, the metadata queue is cleared (`__disagg_shared_metadata_queue_clear` at line 1378), but no test verifies that:
1. The `WT_BTREE_READONLY` flags set during the first step-down are correctly cleared when the node steps up again.
2. The `last_checkpoint_meta_lsn`, `last_checkpoint_root`, and related fields are correctly updated across multiple transitions.
3. The ingest tables that were drained during the first promotion do not have stale state (e.g., incorrect GC prune timestamps) on the second promotion.

**Risk:**
If `WT_BTREE_READONLY` flags are not cleared on the second step-up, the leader would reject writes to stable btrees silently (writes would fail at a lower layer). The prune timestamp on ingest tables is updated via `__wti_layered_iterate_ingest_tables_for_gc_pruning` during checkpoint pickup, but it is unclear whether this is reset correctly when a node steps down and then is promoted again without having picked up a new checkpoint in between (i.e., the node goes back to a checkpoint it already has).

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_mark_btrees_readonly_then_step_down()` lines 1321–1356 — sets `WT_BTREE_READONLY` on all open disaggregated btrees.
- `src/conn/conn_layered.c:__disagg_step_up()` lines 1256–1313 — does NOT explicitly clear `WT_BTREE_READONLY` from existing btrees before attempting `__disagg_restart_checkpoint()`. The drain (`__wti_layered_drain_ingest_tables`) may re-open handles, but any handle that was kept open after step-down and not re-opened will retain the `READONLY` flag.
- `src/conn/conn_layered.c:__disagg_step_down()` line 1378 — clears the metadata queue, but does not zero out `last_checkpoint_root` or timestamps.
- Triggered when: A node is stepped down and back up without a connection close/reopen in between.
- Why current tests miss it: test_layered07 and test_layered26 each do exactly one role swap. test_layered21 does exactly one transition (leader to follower). No test chains two complete transitions on the same connection.

**Proposed test design:**
- Setup: Two connections (leader + follower). 500 records in a layered table, checkpoint.
- Operations: (1) Swap: follower→leader, leader→follower. Insert 100 records from new leader, checkpoint. (2) Swap again: new follower→leader, new leader→follower. Insert 100 more records from the re-promoted node, checkpoint. (3) Advance the new follower to the latest checkpoint.
- Assertions: (1) Both nodes see all 700 records after each swap; (2) writes succeed on the re-promoted leader (verifying `WT_BTREE_READONLY` was cleared); (3) no stale ingest data leaks across transitions.

---

### [CRITICAL] Gap 3: Cold restart with a partially written (incomplete) checkpoint in the page log

**What is not tested:**
All cold-restart tests (test_layered15, test_layered36, test_layered46) simulate a clean node restart: the leader completes a full checkpoint, saves the `checkpoint_meta`, and then the restart picks up that completed checkpoint. No test simulates a crash in the middle of a checkpoint — specifically, a scenario where the page log contains data written *after* the last complete checkpoint record but before a `pl_complete_checkpoint` call.

**Risk:**
The `__disagg_abandon_checkpoint()` function (lines 1152–1193) is called at startup for leaders (`__wti_disagg_conn_config()` lines 1519–1521) and during step-up (`__disagg_restart_checkpoint()` line 1293). Its purpose is to drop all page-log records after the last completion record. If the PALI implementation's `pl_abandon_checkpoint` is absent (the `== NULL` check at line 1172 triggers a warning and returns 0), then writes made during the incomplete checkpoint will still be present in the page log. The code comment at line 1144–1150 states explicitly that this can lead to "illegal delta chains with wrong backlink LSNs, committing updates from incomplete checkpoints, or even data loss."

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_abandon_checkpoint()` lines 1152–1193 — the `pl_abandon_checkpoint == NULL` guard at line 1172 silently treats the operation as a no-op. FIXME-WT-16524 notes this guard should be removed.
- `src/conn/conn_layered.c:__wti_disagg_conn_config()` line 1519–1521 — the `leader && !picked_up` branch calls `__disagg_abandon_checkpoint` at connection open for leaders, meaning a leader that starts without a `checkpoint_meta` config does not necessarily clean up the page log.
- Triggered when: A leader crashes mid-checkpoint (after calling `pl_begin_checkpoint` or after writing some pages but before `pl_complete_checkpoint`), then a new leader is started without calling `pl_abandon_checkpoint` (because the PALI implementation does not support it).
- Why current tests miss it: The palite test backend completes checkpoints cleanly. No test simulates a mid-checkpoint crash.

**Proposed test design:**
- Setup: Leader inserts data, begins checkpoint (intercepted to allow partial write — write some pages but not the completion record to the palite page log).
- Operations: Kill and restart the leader. Restart with the same `checkpoint_meta` from the last *complete* checkpoint (not the partial one).
- Assertions: (1) After restart, data from the partial checkpoint is not visible (data is as of the last complete checkpoint); (2) the `disagg_abandon_checkpoint_succeed` or `disagg_abandon_checkpoint_failed` stat is updated; (3) a new checkpoint after restart succeeds.

---

### [HIGH] Gap 4: Stable-table creation failure mid-loop during cold restart

**What is not tested:**
`__layered_create_missing_stable_tables_helper()` (lines 74–127) iterates over all `layered:` entries in the metadata and calls `__layered_create_missing_stable_table()` for each. If creation of table N fails (e.g., schema error, disk full), the function returns an error with an `WT_ERR_MSG_CHK` at line 108–110. Tables 0 through N-1 have been created, but tables N+1 onward have not. No test verifies the partial-creation state or that a retry (calling the function again) is idempotent for tables that were already created.

**Risk:**
After a partial failure, the connection panic path fires (step-up failure → `__wt_panic()`). If the database is restarted and step-up is retried, `__layered_create_missing_stable_tables_helper()` will try to create all tables again. For tables already created (N-1 and below), `cursor_check->search()` will find them and skip them. This is safe. However, if the creation of table N left partial metadata (e.g., the `layered:` entry was inserted but the `file:` entry was not), the second attempt may fail in a different way. No test exercises this exact scenario with N>1 tables.

**Code path analysis:**
- `src/conn/conn_layered.c:__layered_create_missing_stable_tables_helper()` lines 74–127 — loop over metadata entries with `WT_ERR` exit on single-table failure; no partial rollback of previously created tables.
- `src/conn/conn_layered.c:__disagg_step_up()` line 1302 — calls this function; failure propagates to `__wti_disagg_conn_config()` which panics.
- Triggered when: Multiple tables exist, and one table's stable file creation fails partway through the loop.
- Why current tests miss it: test_layered36 uses only 2 tables and both succeed. No test injects creation failure for a subset of tables.

**Proposed test design:**
- Setup: 5 layered tables with data, checkpoint. Cold restart (no local files).
- Operations: Inject a fault (mock or file-system wrapper) that causes `__wt_schema_create()` to fail for table index 3 of 5. Call `conn.reconfigure(checkpoint_meta=...)`.
- Assertions: (1) The reconfigure raises an error; (2) after removing the fault and retrying, all 5 tables are accessible; (3) verify the 2 successfully created tables (before the fault) were not left in an inconsistent state.

---

### [HIGH] Gap 5: Open transactions during follower promotion

**What is not tested:**
No test verifies the behavior when there are **open (uncommitted) transactions** on the follower at the moment it is promoted to leader. `__disagg_step_up()` calls `__wti_layered_drain_ingest_tables()` which drains the ingest table by moving updates to the stable table. If there are open prepared transactions or uncommitted writes in the ingest table at that moment, the drain may produce incorrect results (e.g., including uncommitted data in the stable btree).

test_layered31 part 6 tests step-up while a cursor is open but is explicitly disabled (FIXME-WT-14545). There is no test for step-up with an open *transaction* (as opposed to just an open cursor).

**Risk:**
If a follower has open prepared transactions when it is promoted, the drain may commit or lose them depending on the transaction visibility rules at drain time. Since the drain runs under its own session with a snapshot, uncommitted writes from other sessions should not be visible — but this is untested. A bug here would silently corrupt data by either including uncommitted changes in the stable btree or failing to drain valid changes.

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_step_up()` line 1306 — calls `__wti_layered_drain_ingest_tables()` while the checkpoint lock is held.
- `src/conn/conn_layered_ingest.c:__layered_drain_ingest_tables()` — iterates over all ingest table entries and moves them to stable btrees. The visibility of updates depends on the session's transaction snapshot.
- Triggered when: Application code holds an open transaction on a follower connection while the orchestration layer triggers promotion.
- Why current tests miss it: All test step-ups are done after committing all writes or closing cursors cleanly.

**Proposed test design:**
- Setup: Follower connection with an open (uncommitted) transaction T1 that has written records K1–K10 to the ingest table.
- Operations: Trigger `conn.reconfigure('disaggregated=(role="leader")')` *while* T1 is still open.
- Assertions: (1) The step-up completes or fails with a defined error (not a crash); (2) if it succeeds, T1's writes are NOT visible in the stable btree (transaction was uncommitted); (3) after committing T1 and checkpointing, K1–K10 are now visible.

---

### [HIGH] Gap 6: Follower that has never picked up any checkpoint

**What is not tested:**
A follower that starts fresh (no local files, no `checkpoint_meta` provided) and attempts to open cursors on layered tables that exist in the shared metadata but have not yet been picked up locally. test_layered26 verifies that a follower sees zero records *after it has created the table locally* but before pickup. No test starts a follower with no local metadata at all and attempts to access a table.

**Risk:**
When a follower with no metadata picks up its first checkpoint, `__disagg_apply_checkpoint_meta()` inserts all table entries as "new" (the `ret == WT_NOTFOUND` branch at line 389–425). If this path is buggy (e.g., fails to create the ingest table, or creates it with the wrong schema), the follower will crash or return errors on subsequent reads. This is the first-ever-pickup path, which is distinct from the incremental-pickup path (an existing follower picking up a newer checkpoint).

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_apply_checkpoint_meta()` lines 389–425 — the `WT_NOTFOUND` branch that handles brand-new tables. It creates ingest tables by calling `__layered_create_missing_ingest_table()` (line 404) and inserts new metadata entries.
- Triggered when: A follower is started for the first time against a page log that already has checkpoints for multiple tables.
- Why current tests miss it: test_layered15 restarts without local files, but it was the node that originally wrote the checkpoint, so it knows the table schemas. A genuine "new follower" starting against an existing cluster is not tested.

**Proposed test design:**
- Setup: Leader creates 3 tables, writes data, takes a checkpoint. Open a *completely separate* follower connection (different `home` directory, no prior history) and provide the leader's checkpoint metadata.
- Operations: On the new follower, call `conn.reconfigure(checkpoint_meta=...)`. Open cursors on all 3 tables and scan.
- Assertions: (1) All tables are accessible; (2) all data written by the leader is visible; (3) ingest tables are correctly created and appear in local metadata.

---

### [HIGH] Gap 7: Checkpoint pickup TOCTOU race — new checkpoint arrives mid-read

**What is not tested:**
`__disagg_pick_up_checkpoint()` is called under the checkpoint lock (line 739–740). However, `__disagg_pick_up_checkpoint_meta()` opens an internal session and acquires the checkpoint lock *around* the actual pickup. There is a window between when `__disagg_pick_up_checkpoint_meta()` validates the LSN ordering (lines 546–560) and when it completes Part 2 (`__disagg_apply_checkpoint_meta()`). If a concurrent PALI write (from the leader) places a new checkpoint record in the page log during this window, the follower may read tables that are inconsistent across the two checkpoints.

Separately, `__disagg_apply_checkpoint_meta()` reads from a checkpoint of the shared metadata table (line 316–322: `cursor` opened on `WT_DISAGG_METADATA_URI/<checkpoint_name>`). If a new checkpoint of the shared metadata table is written between when `metadata_checkpoint_name` is fetched (line 310–314) and when the cursor is opened (line 322), the follower would be reading from a stale view. This is a time-of-check/time-of-use (TOCTOU) issue.

**Risk:**
Medium-risk in production where the leader is actively checkpointing while a follower is picking up the previous checkpoint. The checkpoint lock on the follower side does not prevent the leader from writing to the shared metadata. The inconsistency would manifest as partially applied metadata — some tables at checkpoint N+1, others at checkpoint N — which could cause cursor scans to return inconsistent data.

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_apply_checkpoint_meta()` lines 310–322 — fetches `metadata_checkpoint_name` then opens a cursor. These two operations are not atomic with respect to the leader writing a new checkpoint.
- `src/conn/conn_layered.c:__disagg_pick_up_checkpoint()` line 535 — asserts checkpoint lock is held; this prevents concurrent *local* checkpoints but does not prevent the leader from writing a new complete checkpoint to the page log.
- Triggered when: Leader completes a checkpoint while a follower's `__disagg_pick_up_checkpoint()` is running.
- Why current tests miss it: All test scenarios are single-threaded: the leader checkpoints, then the follower picks up. No test runs a concurrent leader checkpoint during a follower pickup.

**Proposed test design:**
- Setup: Leader actively writing checkpoints in a background thread (every 100ms). Follower continuously calling `disagg_advance_checkpoint()` in a loop.
- Operations: Run for 30 seconds with concurrent leader writes and follower pickups.
- Assertions: After each pickup, the follower's cursor scan returns a consistent view (no record gaps, no partial metadata state). Use checksums or record counts to detect inconsistency.

---

### [MEDIUM] Gap 8: Table drop and recreate — stale state in table manager

**What is not tested:**
The `WT_LAYERED_TABLE_MANAGER` (in `conn_layered_table_manager.c`) tracks open layered tables by `ingest_id`. When a table is closed (via `__wt_layered_table_manager_remove_table()`), its entry is freed. If the same table is recreated with a different `ingest_id` (which can happen after a cold restart, since file IDs are reassigned), the new entry must be added at the new index. No test verifies that `drop + recreate` of a layered table produces a correct manager state with no dangling pointers or double-add panics.

**Risk:**
The sanity check at `conn_layered_table_manager.c` line 90–92 panics if `entries[ingest_id] != NULL` on add. If the delete path fails to clear the entry before the add is called (e.g., due to a handle not being fully closed), the panic fires and the connection dies. This is a correctness hazard during schema change workloads (drop+recreate under load).

**Code path analysis:**
- `src/conn/conn_layered_table_manager.c:__wt_layered_table_manager_add_table()` lines 52–103 — panics if `entries[ingest_id] != NULL` (line 90–92).
- `src/conn/conn_layered_table_manager.c:__wt_layered_table_manager_remove_table()` lines 139–153 — removes entry by `ingest_id`; relies on dhandle close to call this.
- Triggered when: A layered table is dropped while a handle is still cached (sweep not yet run), then recreated and the new handle is opened before the old handle's close path fires.
- Why current tests miss it: No test does drop+recreate of a layered table; tests only create or drop, not both in sequence.

**Proposed test design:**
- Setup: Leader with 3 layered tables, data, checkpoint.
- Operations: Drop table 2. Immediately recreate table 2 (same URI, same schema). Insert data, checkpoint.
- Assertions: (1) No panic on recreate; (2) old data is not visible after recreate; (3) new data is checkpointed and visible after a follower picks up.

---

### [MEDIUM] Gap 9: Connection open race with concurrent page log writes

**What is not tested:**
At connection open, `__wti_disagg_conn_config()` calls `__disagg_pick_up_checkpoint_meta()` (line 1531) if `checkpoint_meta` is provided. The internal session opened at line 736 acquires the checkpoint lock before doing any work. However, if the leader is simultaneously writing a new checkpoint to the page log, the follower may read a checkpoint whose constituent pages are still being written (i.e., the metadata LSN is complete but some leaf pages are still in flight). The page log's read path must handle this case, but it is not verified in any test.

**Risk:**
If the PALI layer does not guarantee that all pages up to a given complete-checkpoint record are durably readable before returning from `pl_complete_checkpoint`, a follower that picks up that checkpoint immediately may see missing or partial pages when eviction re-reads them from the page log. This is a correctness issue for the PALI contract, not for WiredTiger's own code, but WiredTiger tests should validate the contract.

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_pick_up_checkpoint_meta()` lines 683–748 — called at connection open if `checkpoint_meta` is provided; acquires checkpoint lock.
- `src/conn/conn_layered.c:__wt_disagg_advance_checkpoint()` lines 1822–1898 — the leader calls `pl_complete_checkpoint` (line 1877) and then `pl_begin_checkpoint` (line 1894). Between these two, a follower may attempt to open the just-completed checkpoint.
- Triggered when: Production scenario where the leader completes a checkpoint and the orchestration layer immediately promotes a new follower with that checkpoint's metadata.
- Why current tests miss it: All test scenarios serialize: leader checkpoints, follower picks up. No concurrent access during the checkpoint completion window is tested.

**Proposed test design:**
- Setup: Leader actively checkpointing in a tight loop. On a separate thread, spawn follower connections that immediately pick up the latest checkpoint.
- Operations: Run 20 iterations of leader-checkpoint + follower-pickup with minimal delay between completion and pickup.
- Assertions: Each follower can scan all tables and see consistent data; no WT_PANIC or assertion failures.

---

### [MEDIUM] Gap 10: Two consecutive cold restarts

**What is not tested:**
test_layered15 performs two cold restarts in sequence, but both restarts use the same pattern: restart, pick up checkpoint, become leader, write data, checkpoint. No test verifies what happens if a node performs two cold restarts *without becoming leader between them* — i.e., a follower restarts, picks up checkpoint N, then immediately restarts again and picks up checkpoint N+1 (or even checkpoint N again, idempotent case).

**Risk:**
On the second restart, `__disagg_apply_checkpoint_meta()` must handle the case where the local metadata already has entries for the tables (from the first restart's pickup). The code at line 334 handles "existing table" (searches succeed) by updating the checkpoint config. If the second pickup has a lower or equal LSN, the LSN check at line 546 returns `EINVAL`. If the second pickup is of the *same* checkpoint (same LSN), the warning path at line 555 fires. These paths are indirectly exercised by test_layered53 (idempotent pickup), but not in the context of a cold restart with no local metadata state.

**Code path analysis:**
- `src/conn/conn_layered.c:__disagg_pick_up_checkpoint()` lines 544–560 — LSN ordering checks; same-LSN is a warning, lower LSN is an error.
- Triggered when: A follower restarts twice without the leader writing a new checkpoint in between.
- Why current tests miss it: test_layered15's two restarts always use a fresh `checkpoint_meta` because the leader wrote a new checkpoint between them.

**Proposed test design:**
- Setup: Leader creates tables, checkpoints. Save `checkpoint_meta_A`.
- Operations: Cold restart as follower. Pick up `checkpoint_meta_A`. Cold restart again (no leader checkpoint in between). Pick up `checkpoint_meta_A` again.
- Assertions: (1) The second pickup of the same checkpoint logs "Picking up the same checkpoint again" and returns success; (2) data is still accessible; (3) `last_checkpoint_meta_lsn` is not changed.

---

### [LOW] Gap 11: Large-table-count with diverse sizes — resource limits in production

**What is not tested:**
test_layered29 creates 10,000 empty tables (no data). It tests table ID allocation and metadata scalability but does not test the combined stress of many tables each with significant data and checkpoint history. In production, the concern is the in-memory footprint of the table manager entries, the page log metadata table size, and the time taken by `__layered_create_missing_stable_tables_helper()` on a cold restart when iterating over thousands of tables.

**Risk:**
Low for pure correctness, but the table manager array is allocated with `calloc(ingest_id * 2)` (line 85 of `conn_layered_table_manager.c`) on resize, which could allocate excessive memory if `ingest_id` values are sparse (e.g., due to dropped tables with high IDs). Additionally, the cold-restart stable-table creation loop is O(N) in the number of tables with schema lock held throughout. For N=10,000 tables this could stall the connection for a significant time.

**Code path analysis:**
- `src/conn/conn_layered_table_manager.c:__wt_layered_table_manager_add_table()` line 84–87 — doubles the array size to `ingest_id * 2`, which can be large if IDs are sparse.
- `src/conn/conn_layered.c:__layered_create_missing_stable_tables_helper()` lines 74–127 — linear scan holding schema lock.
- Triggered when: Many tables with high file IDs (due to drop history) cause sparse ID space.
- Why current tests miss it: test_layered29 creates all tables without dropping any, so IDs are dense.

**Proposed test design:**
- Setup: Create 1000 tables, checkpoint, drop every other table, create 1000 more tables. Repeat 3 cycles.
- Operations: Cold restart. Pick up the last checkpoint.
- Assertions: (1) Cold restart completes within a time limit; (2) table manager array size is bounded (not quadratic in the number of drops); (3) all surviving tables are accessible.

---

## Summary Table

| Priority | Gap | Risk |
|---|---|---|
| CRITICAL | Step-up failure leaves node with `leader=true` but corrupted state; panic is only defense | Data corruption / silent leader with no valid checkpoint |
| CRITICAL | Multiple consecutive role transitions (leader→follower→leader) — `WT_BTREE_READONLY` not cleared | Silent write failures on re-promoted leader |
| CRITICAL | Cold restart with partially written checkpoint — `pl_abandon_checkpoint` may be a no-op | Data from incomplete checkpoint committed; delta chain corruption |
| HIGH | Stable-table creation failure mid-loop during cold restart — partial table set created | Inconsistent metadata; panic on retry; tables inaccessible |
| HIGH | Open uncommitted transactions on follower at promotion time | Uncommitted data in stable btree; or committed data lost |
| HIGH | Follower that has never picked up any checkpoint tries to access tables | First-pickup path untested; any bug here blocks all new followers |
| HIGH | Checkpoint pickup TOCTOU: new checkpoint arrives while follower mid-read | Inconsistent metadata partially at checkpoint N, partially at N+1 |
| MEDIUM | Table drop-and-recreate — stale table manager entry causes panic on add | Spurious connection panic during schema change workloads |
| MEDIUM | Connection open race with concurrent leader page log writes | Partial page reads on follower cold-start |
| MEDIUM | Two consecutive cold restarts without new leader checkpoint between them | Same-checkpoint pickup path untested in cold-restart context |
| LOW | Large table count with sparse IDs (due to drop history) — table manager array size, cold restart latency | Excessive memory; cold restart stall under schema lock |
