# Gap Analysis: Disagg Schema, Metadata, Verify, and Crash Recovery

---

## Current Coverage Summary

### Schema / Metadata
| Test | What it covers |
|---|---|
| test_layered24 | Follower drop must not fall back to stable in page log; leader drop cleanly removes table |
| test_layered28 | Drop: local metadata cleanup, shared-metadata persistence after checkpoint, follower-drop must NOT touch shared metadata, sweep-thread safety after drop |
| test_layered29 | Bulk creation of 10 000 layered tables (scale) |
| test_layered30 | Empty table: checkpoint, follower pickup, cold restart |
| test_layered36 | Missing stable tables re-created from page log on cold restart |
| test_layered40 | `log=(enabled=false)` enforced on both constituent files |
| test_layered51 | `log=(enabled=true)` rejected with correct error message |
| test_layered_incomplete_table (Catch2) | Partial metadata on reopen (leader: aborts if either file missing; follower: aborts only if ingest missing, tolerates missing stable) |
| test_disagg_meta_config (Catch2) | Disagg checkpoint-turtle metadata parsing, version/compat checks, crypt metadata |
| test_layered87 | `simulate_crash_restart` + RTS skipped for layered; basic crash-then-reopen with a single non-schema workload |

### Verify
| Test | What it covers |
|---|---|
| test_verify_disagg | verify on empty leader, follower before/after checkpoint pickup, dirty-data EBUSY, HS-populated variant |
| test_verify_disagg02 | Duplicate btree-ID detection via injected metadata corruption |

### Crash Recovery (csuite)
| Test | Disagg support |
|---|---|
| timestamp_abort | Partial: `-G` flag enables disagg mode, creates `type=layered` collection table, runs full SIGKILL→recovery→verify loop. Column-store, LazyFS, and backup sub-scenarios are explicitly blocked for disagg. Schema-create/drop threads NOT run in disagg mode. |
| random_abort | No disagg support at all |
| schema_abort | No disagg support at all |
| truncated_log | Not applicable (layered tables do not use WAL) |

---

## Duplicate / Overlapping Cases

1. **Drop semantics on follower** — both `test_layered24` and `test_layered28.test_create_drop_follower` verify that a follower drop does not access stable data and does not propagate to shared metadata. The distinction is minor (test_layered24 uses 30 000 records, test_layered28 uses 1 000). Consider consolidating into a single parametrised test.

2. **Missing constituent on reopen** — `test_layered36` (Python, integration) and `test_layered_incomplete_table` (Catch2, unit) both verify that stable tables can be re-created on cold restart. They approach the same invariant from different angles (integration vs. isolated unit); keeping both is reasonable but the Python test adds no assertion about the _content_ of the re-created stable table beyond a single key-value pair.

3. **`log=false` enforcement** — `test_layered40` (metadata cursor check) and `test_layered51` (API rejection) both guard the same invariant. No consolidation needed because they test different entry points, but they should be noted as paired coverage.

---

## Missing Coverage

### [CRITICAL] Gap 1: Partial `__create_layered` failure — ingest created, stable creation fails

**What is not tested:**
`__create_layered` in `src/schema/schema_create.c` writes the layered metadata entry first (`__wt_metadata_insert` at line 1194), then creates the ingest file (`__wt_schema_create` at line 1208), then creates the stable file (line 1217) and enqueues the shared-metadata operation (line 1226). If creation of the stable file fails (e.g., ENOSPC, block-manager error, page-log extension failure), the layered metadata entry and the ingest file already exist in local metadata. The `goto err` path frees scratch buffers but does NOT roll back the already-inserted layered metadata record or the already-created ingest file. Meta-tracking may or may not undo these depending on whether a schema transaction is active, but this code path is entirely untested.

**Risk:**
Orphaned `layered:T` and `file:T.wt_ingest` metadata entries with no corresponding `file:T.wt_stable`. On next reopen as leader this triggers `WT_ASSERT_ALWAYS` in `__metadata_clean_incomplete_table` (per `test_layered_incomplete_table`), crashing the process. On the follower the same condition silently passes, meaning the table appears to exist but can never serve stable data. Data loss and unrecoverable database state.

**Code path analysis:**
- Source: `src/schema/schema_create.c`, function `__create_layered`, lines 1193–1228
- Specific path: metadata insert (line 1194) → ingest create (line 1208) → `__wt_free(session, constituent_cfg)` (line 1209) → stable create (line 1217). If line 1217 returns an error, `goto err` at line 1230 does NOT call `__wt_metadata_remove` for the layered URI or for the ingest URI.
- Why tests miss it: All existing tests assume successful creation. No fault-injection harness exists for the stable-file creation path under disaggregated storage.

**Proposed test design:**
- Setup: Leader connection with a page-log extension that can be instructed to fail on the N-th `create` call for a `.wt_stable` file.
- Operations: (1) Create layered table — expect failure. (2) Reopen the connection. (3) Verify neither `layered:T`, `file:T.wt_ingest`, nor `file:T.wt_stable` appear in local or shared metadata (i.e., the partial state was rolled back). (4) Verify a fresh `session.create` of the same URI succeeds.
- Assertions: No orphaned metadata entries survive the failed create; no assert-abort on reopen; table is re-creatable after the failure.


### [CRITICAL] Gap 2: Leader crash mid-drain (step-up drain interrupted)

**What is not tested:**
During step-up (`__disagg_step_up` in `src/conn/conn_layered.c`, lines 1252–1313), the leader (a) abandons any incomplete checkpoint, (b) creates missing stable tables, then (c) calls `__wti_layered_drain_ingest_tables`. If the process crashes after step-up is committed to `conn->layered_table_manager.leader = true` (line 1285) but before drain completes, the ingest tables on the ex-follower hold data that has never been written to any stable file or page log. There is a FIXME-WT-14734 comment in the drain code noting that the table manager lock is released early. No test exercises a crash in this window.

**Risk:**
After the crash the node restarts as leader. The stable tables contain only the data that was drained before the crash; the ingest tables are empty (in-memory, lost on crash). Data written since the last checkpoint but before step-up is permanently lost with no error. The page log has no record of this data either.

**Code path analysis:**
- Source: `src/conn/conn_layered.c`, `__disagg_step_up`, lines 1285–1307; `src/conn/conn_layered_ingest.c`, `__wti_layered_drain_ingest_tables`, lines 617–700
- Specific path: `leader = true` (line 1285) → `__disagg_restart_checkpoint` (line 1293) → `__layered_create_missing_stable_tables` (line 1302) → `__wti_layered_drain_ingest_tables` (line 1306). Crash anywhere after line 1285 and before drain commits to the page log.
- Why tests miss it: `test_layered87` uses `simulate_crash_restart` but does so on a stable leader, not during step-up. No test initiates a step-up and crashes mid-drain.

**Proposed test design:**
- Setup: Two connections — a long-running leader that writes and checkpoints data, and a follower that steps up. Use a timing-stress injection point (or a signal handler) to kill the process at drain iteration N of M tables.
- Operations: (1) Follower writes uncommitted data to ingest tables. (2) Follower initiates step-up. (3) Kill the process mid-drain. (4) Reopen as leader. (5) Verify: data that was in ingest tables before step-up is either fully present (if drain completed for that table) or absent but recoverable from the previous checkpoint.
- Assertions: No assert-abort on reopen. Data in the page log (checkpointed before the crash) is intact. Data that was only in ingest and never drained is correctly absent and does not appear as corrupted.


### [CRITICAL] Gap 3: Leader crash during page-log write (mid-checkpoint)

**What is not tested:**
`timestamp_abort` with `-G` (disagg mode) runs the full SIGKILL→recovery loop, but it only creates a single `type=layered` table and the crash is not orchestrated to land specifically during the page-log write phase (`pl_complete_checkpoint` / PALI flush). There is no test that: (a) begins a checkpoint, (b) crashes after some stable-file pages are written to the page log but before the checkpoint completion record is written, and then (c) verifies that the next leader correctly calls `pl_abandon_checkpoint` and restarts a clean checkpoint.

**Risk:**
If the abandon-checkpoint path (`__disagg_abandon_checkpoint`) is incorrectly invoked or the PALI implementation has a bug where it does not properly identify the incomplete checkpoint boundary, the recovered leader could read partially-written page-log entries as valid data. This produces silent data corruption — reads return data that was never committed.

**Code path analysis:**
- Source: `src/conn/conn_layered.c`, `__disagg_abandon_checkpoint` (line 1152) and `__disagg_restart_checkpoint` (line 1237)
- Specific path: checkpoint begins → PALI `pl_begin_checkpoint` → pages written → process killed → reopen → `__disagg_restart_checkpoint` called → `pl_abandon_checkpoint` invoked → `pl_begin_checkpoint` on fresh checkpoint
- Why tests miss it: `timestamp_abort -G` crashes at random points, but the verification only checks that the recovered collection table contains data up to the last stable timestamp. It does not verify that the page log itself is in a consistent state (no partial checkpoint data visible). FIXME-WT-16524 notes that `pl_abandon_checkpoint` is not yet universally supported by all PALI implementations, so the current code has a silent no-op fallback.

**Proposed test design:**
- Setup: Leader writing data across multiple checkpoints. Use a PALI hook or fault injector to fail the `pl_complete_checkpoint` call (simulating a crash after some page writes).
- Operations: (1) Write data. (2) Start checkpoint. (3) Inject failure mid-checkpoint. (4) Reopen as leader. (5) Verify that `pl_abandon_checkpoint` was called and that no data beyond the last complete checkpoint is visible.
- Assertions: Data visible post-recovery matches exactly the last complete checkpoint. Page log has no entries referencing the abandoned checkpoint's LSN range. The `disagg_abandon_checkpoint_succeed` stat is incremented.


### [CRITICAL] Gap 4: `rename` of a layered table is silently unsupported

**What is not tested:**
`__schema_alter` in `src/schema/schema_alter.c` (line 389–431) has explicit handlers for `file:`, `colgroup:`, `index:`, `object:`, `table:`, `tier:`, and `tiered:`, but no handler for `layered:`. Calling `session->alter` on a `layered:` URI falls through to `__wt_bad_object_type`. Similarly, there is no `schema_rename.c` file (the schema directory contains no rename source), and the rename entry point in `session_api.c` routes through the schema dispatch which has the same gap. No test verifies what happens when you try to alter or rename a layered table.

**Risk:**
- `alter` on `layered:T` returns `EINVAL` ("unexpected object type") without touching shared metadata. If the caller retries on the underlying `file:T.wt_stable` URI directly and succeeds, the shared metadata and local metadata are now inconsistent.
- If `rename` is ever called on a layered table (e.g., during a live migration), the `layered:` metadata entry is not found by the rename dispatcher, so it returns an error — but the error message may be confusing and the correct behavior (rejecting with a clear "unsupported" error vs. transparently renaming all sub-components) is not documented or tested.

**Code path analysis:**
- Source: `src/schema/schema_alter.c`, `__schema_alter`, line 430: `return (__wt_bad_object_type(session, uri))`
- Source: `src/schema/schema_worker.c`, `__wt_schema_worker`, line 270: `layered:` is only handled when `file_func == __wt_verify`. For `__wt_salvage` or any other worker op the code falls through to `__wt_bad_object_type`.
- Why tests miss it: All layered schema tests (test_layered28, 24, etc.) only call `create` and `drop`. No test calls `session.alter` or `session.rename` on a `layered:` URI.

**Proposed test design:**
- Setup: Create a layered table with a specific collation or format config.
- Operations: (1) `session.alter("layered:T", "cache_resident=true")` — assert returns `EINVAL` with a useful message, not a crash. (2) `session.alter("layered:T", "log=(enabled=true)")` — assert returns `EINVAL` (not a silent no-op). (3) Attempt `session.rename("layered:T", "layered:T2")` — assert returns `EINVAL` (not a crash, not silent success leaving orphaned metadata).
- Assertions: No crash. Shared metadata unchanged after failed alter/rename. Local metadata unchanged.


### [HIGH] Gap 5: Schema operations during active drain

**What is not tested:**
`__wti_layered_drain_ingest_tables` in `src/conn/conn_layered_ingest.c` snapshots the table count at line 633 then immediately releases the `layered_table_lock` (line 639), with an explicit FIXME-WT-14734 comment: "shouldn't we hold this lock longer, e.g. manager->entries could get reallocated, or individual entries could get removed or freed." A concurrent `session.drop` during drain could free an entry that the drain worker is referencing. Similarly, a `session.create` of a new layered table during drain is not added to the in-progress drain batch, meaning the new table's ingest data is drained in the *next* checkpoint cycle, not the current one.

**Risk:**
- Drop during drain: use-after-free of `WT_LAYERED_TABLE_MANAGER_ENTRY`. Not currently caught by TSAN or address sanitizer because the window is narrow and tests run drain sequentially.
- Create during drain: new table's ingest data is silently skipped during this drain cycle. If the leader crashes immediately after drain completes but before the next checkpoint, the new table's ingest data is lost.

**Code path analysis:**
- Source: `src/conn/conn_layered_ingest.c`, `__wti_layered_drain_ingest_tables`, lines 631–639 (lock released before worker loop) and lines 663–677 (worker accesses `manager->entries[i]` without holding the lock)
- Why tests miss it: All drain tests run sequentially — step-up is single-threaded and no concurrent DDL is issued during the drain window.

**Proposed test design:**
- Setup: Leader with multiple layered tables. Use multiple Python threads or a C csuite test with concurrent threads.
- Operations: Thread 1: call `conn.reconfigure(disaggregated=(role="leader"))` to trigger step-up + drain. Thread 2: concurrently issue `session.drop("layered:T2")` on a different table while drain is running.
- Assertions: No crash (ASAN/TSAN clean). After step-up completes, the dropped table is inaccessible. All remaining tables have correct data in their stable components. No orphaned handles.


### [HIGH] Gap 6: `import` into a layered / disagg connection

**What is not tested:**
`__create_file` in `src/schema/schema_create.c` handles `import` via the `WT_SESSION_IMPORT` flag (line 219). The import path checks for tiered-object files (line 310–313) and blocks them, but there is no analogous block for `.wt_stable` or `.wt_ingest` suffixes. The code at line 89 detects `.wt_stable` files and routes them through the disaggregated block manager, but importing a foreign `.wt_stable` file from another database (with a different page-log history) is not blocked or warned against. Importing a regular btree file as a constituent of a layered table is also unexplored.

**Risk:**
Importing a `.wt_stable` file from a different page-log history could produce undetectable data corruption — the block manager may read pages using LSNs that don't correspond to any record in the current page log. The missing test means this code path has never been exercised in a disagg context.

**Code path analysis:**
- Source: `src/schema/schema_create.c`, `__create_file`, lines 265–380
- Source: `src/schema/schema_create.c`, lines 89–107 (`.wt_stable` detection and disagg block manager routing)
- Why tests miss it: No test attempts `session.create` with `import=(file_metadata=...)` on a `layered:` URI or on a `.wt_stable` file URI in a disagg connection.

**Proposed test design:**
- Setup: Create a layered table in one WiredTiger home, checkpoint it, then attempt to import the resulting `.wt_stable` file into a second, fresh disagg connection.
- Operations: `session.create("file:T.wt_stable", "import=(file_metadata=...)")` on the second connection.
- Assertions: Either (a) the import is correctly rejected with a clear `ENOTSUP` or `EINVAL` error, or (b) if import is supported, verify that pages are readable and that the page-log metadata is correctly updated to reference the new connection's page log.


### [HIGH] Gap 7: `verify` does not check page-log delta-chain consistency

**What is not tested:**
`__wt_verify` in `src/btree/bt_vrfy.c` walks the B-tree and calls `__verify_disagg_accumulate_size` to accumulate block sizes for comparison with the checkpoint size field (lines 422–437). It logs disagg metadata per page (`delta_count`, `backlink_lsn`, `base_lsn`, etc.) via `__verify_disagg_string` (line 617). However, there is no assertion that the `backlink_lsn` of each page actually points to a valid LSN that exists in the page log, that the `delta_count` is consistent with the chain of deltas actually stored, or that applying the delta chain reconstructs the base page correctly. The checkpoint size mismatch check is currently disabled with `if (false)` (line 433).

**Risk:**
A corrupted or truncated delta chain would not be detected by `verify`. The corruption would only surface when a page is read and PALI fails to reconstruct it from deltas — at which point the database is likely unusable.

**Code path analysis:**
- Source: `src/btree/bt_vrfy.c`, lines 420–437 (checkpoint size check disabled), lines 575–629 (`__verify_disagg_string` only formats metadata for display, does not assert validity)
- Source: `src/btree/bt_read.c`, line 361: delta verification only happens during page read, not during `verify`
- Why tests miss it: `test_verify_disagg` verifies that `verify` succeeds and returns the correct error codes, but it does not inject delta corruption and check whether `verify` detects it.

**Proposed test design:**
- Setup: Leader writes data that produces delta pages (multiple updates to the same key across checkpoints to trigger deltas). Run `verify` — assert success.
- Corruption: Directly corrupt a `backlink_lsn` or `delta_count` field in the PALI page-log storage for one page.
- Operations: Run `verify` again.
- Assertions: `verify` returns `WT_ERROR` or `EINVAL`. The specific page and its disagg metadata are logged. No crash.


### [HIGH] Gap 8: `verify` on follower that has never picked up any checkpoint (stable URI missing entirely)

**What is not tested:**
`test_verify_disagg.test_verify_follower_no_checkpoint` tests the case where the follower has **created** the layered URI locally before receiving any checkpoint, and asserts that verify succeeds (ENOENT on the missing stable file is silently suppressed, per `__schema_layered_stable_worker_verify` line 87). What is not tested: (a) verify on a follower that has **not** created the URI locally — this is `test_verify_follower_no_metadata` which correctly expects ENOENT. The gap is the intermediate case: (b) the follower has received one checkpoint (stable file exists) but the stable file's page-log content was written by a leader that has since been replaced, and the current page-log is empty or truncated. In this scenario `verify` calls `bm->verify_start` on a `.wt_stable` file whose checkpoints reference LSNs that no longer exist.

**Risk:**
`verify` silently returns success even though the stable file's data is unreadable. This is the exact scenario where the checkpoint-size check disabled at line 433 would matter.

**Code path analysis:**
- Source: `src/schema/schema_worker.c`, `__schema_layered_stable_worker_verify`, lines 86–92 (ENOENT suppressed on follower)
- Source: `src/btree/bt_vrfy.c`, lines 395–400 (empty root page skipped without error)
- Why tests miss it: Existing verify tests control the page-log state through `disagg_advance_checkpoint`. None of them artificially truncate or replace the page log between checkpoints.

**Proposed test design:**
- Setup: Leader writes and checkpoints data. Follower picks up the checkpoint. Leader is replaced (new page-log environment or page-log is reset). Follower runs `verify`.
- Assertions: `verify` should return an error (not silent success) when the stable file's checkpoint LSNs are not resolvable from the current page log.


### [HIGH] Gap 9: `alter` of a layered table — config not propagated to constituents

**What is not tested:**
Even if `session.alter` on `layered:T` were to succeed (it currently hits `__wt_bad_object_type`), there is no test verifying that a config change (e.g., `cache_resident`) is propagated to both `file:T.wt_ingest` and `file:T.wt_stable`. For the `table:` prefix form of a layered table, `session.alter("table:T")` hits `__alter_table` which iterates column groups — the column group source for a layered table is the layered handle itself, not the stable file directly, so the propagation chain is different from a regular table. This is untested.

**Risk:**
Config changes silently apply to the `table:` metadata entry but not to the underlying `file:T.wt_stable` constituent, leaving the two metadata records inconsistent. After a reopen, the merged config produces unexpected behavior (e.g., `cache_resident` set at the table level but not at the file level).

**Code path analysis:**
- Source: `src/schema/schema_alter.c`, `__alter_table`, lines 330–383
- Source: `src/schema/schema_alter.c`, `__schema_alter`, line 424: routes `table:` through `__alter_table`, but for `layered:` falls through to `__wt_bad_object_type` (line 430)
- Why tests miss it: No test calls `session.alter` on any layered table URI.

**Proposed test design:**
- Setup: Create a layered table via both `layered:` and `table:` prefixes.
- Operations: (1) `session.alter("table:T", "cache_resident=true")` — check that metadata entries for `table:T`, `layered:T` (if present), `colgroup:T`, `file:T.wt_stable`, and `file:T.wt_ingest` all reflect the new config. (2) `session.alter("layered:T2", "cache_resident=true")` — assert this returns a clear, documented error (either `EINVAL` or `ENOTSUP`).
- Assertions: No silent config divergence between the layered handle and its constituents. Shared metadata updated if applicable.


### [MEDIUM] Gap 10: `schema_abort`-style crash recovery for disagg schema operations

**What is not tested:**
`schema_abort` (csuite) exercises concurrent create/drop operations across a SIGKILL. There is no disagg-equivalent: no test runs concurrent layered-table create/drop with SIGKILL recovery. The shared-metadata queue (`disaggregated_storage.shared_metadata_qh`) is an in-memory structure that is populated during `drop` or `create` and flushed to `WiredTigerShared.wt_stable` at the next checkpoint. If the process is killed after a local metadata `drop` but before the checkpoint that would flush the queue, the shared metadata still contains the dropped table's entries. On reopen, `__metadata_clean_incomplete_table` would not detect the orphan (the local metadata sees the drop as complete, but the shared metadata still has the entry).

**Risk:**
After a crash mid-drop, a new leader sees the table in shared metadata but not in local metadata. Followers that advance their checkpoint get the table re-created locally, creating a "ghost table" that was supposed to be dropped. Data integrity violation: users believe the table is gone, but followers can still read its data.

**Code path analysis:**
- Source: `src/conn/conn_layered.c`, `__wt_disagg_enqueue_metadata_operation`, lines 856–868 (drop is deferred if checkpoint is running) and `__disagg_shared_metadata_queue_clear`, lines 898–916
- Source: `src/schema/schema_drop.c`, `__drop_layered`, lines 174–227
- Why tests miss it: `schema_abort` has no `-G` disagg flag. `test_layered28` tests the happy-path two-checkpoint sequence but not a crash between the drop and the flush checkpoint.

**Proposed test design:**
- Setup: C csuite test (or Python with subprocess) running concurrent layered-table creates/drops with periodic SIGKILL.
- Operations: Each iteration: create table → insert data → checkpoint → drop table → SIGKILL (before second checkpoint that would flush the drop to shared metadata).
- Recovery: Reopen as leader. Scan shared metadata and local metadata for consistency. Advance a follower connection to the recovered checkpoint. Verify the follower does not see a ghost table.
- Assertions: Local metadata and shared metadata agree on which tables exist. No ghost tables visible on follower. No assert-abort on reopen.


### [MEDIUM] Gap 11: `timestamp_abort` disagg mode — schema operations missing

**What is not tested:**
`timestamp_abort` with `-G` (disagg) creates tables only on the first iteration and never runs the schema-operations thread (which creates/drops short-lived tables between checkpoints). The schema thread is silently skipped in the disagg path. This means the disagg variant of `timestamp_abort` does not test what happens when a table is created, data is written to it, and then the process is killed before a checkpoint that would capture both the schema operation and the data.

**Risk:**
In disagg mode, a table created after the last checkpoint but before a SIGKILL leaves a `layered:` metadata entry and an `ingest` file in local metadata, but no corresponding entry in shared metadata (the shared-metadata enqueue happens at checkpoint time). On recovery the `__metadata_clean_incomplete_table` logic should detect and remove this orphan, but this path is never exercised under load.

**Code path analysis:**
- Source: `test/csuite/timestamp_abort/main.c`, line 963–984: schema thread is absent for disagg mode. Lines 1564–1584: disagg setup only enables one `type=layered` table.
- Why tests miss it: Explicit design choice in the test to avoid schema complexity in disagg mode.

**Proposed test design:**
- Modify `timestamp_abort -G` to add an optional schema-operations thread (or add a `-S` flag) that creates and drops short-lived layered tables between the main checkpoint cycles.
- Assertions: After SIGKILL + recovery, no orphaned layered metadata entries exist. Tables created before the last checkpoint are either fully present (if they survived) or cleanly absent (if their create was not checkpointed). No assert-abort.


### [LOW] Gap 12: Verify after step-up (follower→leader transition)

**What is not tested:**
There is no test that runs `verify` immediately after `conn.reconfigure(disaggregated=(role="leader"))` (step-up) before the first post-step-up checkpoint. At this point the node has drained ingest to stable, but the stable file has not yet been included in a new checkpoint. Verify at this moment exercises the code path where `__wt_meta_ckptlist_get` finds checkpoints from the follower era.

**Risk:**
`verify` may read stale follower-era checkpoint metadata referencing LSNs that are no longer the current checkpoint. The result could be a false-positive success (verify walks an old checkpoint and finds it intact, even though the drained data is not yet checkpointed) or a spurious ENOENT/EBUSY error.

**Code path analysis:**
- Source: `src/btree/bt_vrfy.c`, lines 354–362 (`WT_NOTFOUND` from `__wt_meta_ckptlist_get` returns success for empty objects; ingest tables are expected to have no checkpoints)
- Why tests miss it: All verify tests checkpoint before and after major state transitions. None call verify in the post-step-up, pre-checkpoint window.

**Proposed test design:**
- Setup: Follower with data. Step up to leader (drain completes). Call `verify` immediately before the first post-step-up checkpoint.
- Assertions: Verify returns 0 (success) and the data visible post-step-up matches what was in the ingest tables.

---

## Summary Table

| Priority | Gap | Risk |
|---|---|---|
| CRITICAL | Partial `__create_layered` failure leaves orphaned metadata | Unrecoverable database state (assert-abort on reopen) |
| CRITICAL | Leader crash mid-drain during step-up | Silent data loss — ingest data lost permanently |
| CRITICAL | Leader crash during page-log write (mid-checkpoint) | Silent data corruption — partial checkpoint visible post-recovery |
| CRITICAL | `rename`/`alter` of `layered:` URI silently unsupported — no test documents behavior | Potential metadata inconsistency or crash if callers assume support |
| HIGH | Schema operations (drop/create) concurrent with active drain | Use-after-free (drop) or silent ingest data skip (create) |
| HIGH | `import` into a disagg connection — `.wt_stable` file not blocked | Silent data corruption from cross-database page-log LSN mismatch |
| HIGH | `verify` does not check delta-chain consistency | Corrupted deltas undetected; only surface during read (too late) |
| HIGH | `verify` on follower with stale/truncated page log returns silent success | False positive verify; data unreadable but not detected until read |
| HIGH | `alter` config change not propagated to both stable and ingest constituents | Silent config divergence after reopen |
| MEDIUM | No disagg equivalent of `schema_abort` (concurrent DDL + SIGKILL) | Ghost tables visible on followers after crash mid-drop |
| MEDIUM | `timestamp_abort -G` skips schema-operations thread | Unexercised orphaned-table recovery path after SIGKILL |
| LOW | `verify` immediately post step-up (pre-checkpoint) — behavior undocumented | False positive or spurious error in a valid operational window |
