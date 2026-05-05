# Gap Analysis: Disaggregated Storage — Cross-Cutting Feature Coverage

*Evaluates which general WT features lack disagg-specific test coverage*

*Generated: 2026-05-01*

---

## Coverage Matrix

| Feature | General Tests | Disagg Tests | hook_disagg coverage | Disagg-specific src/ code | Assessment |
|---|---|---|---|---|---|
| Backup (full + incremental) | test_backup01–30 | none | skipped entirely | none (backup has no disagg code) | CRITICAL GAP |
| History Store | test_hs01–33, test_hs_evict_race01 | test_layered25 (HS after restart) | hook skips test_backup (HS tests run) | `hs_conn.c`: shared HS table (`WiredTigerSharedHS.wt_stable`) created for disagg | PARTIAL — shared HS write/read correctness not verified independently |
| Prepared transactions | test_prepare01–45, test_prepare_hs*, test_prepare_cursor* | test_layered45 (delta skip), test_layered65 (GC), test_layered94 (step-up), test_layered69, test_layered73, test_layered89 | general prepare tests run via hook | reconcile visibility (`rec_visibility.c`): disagg flag gates prepared-update path | MOSTLY COVERED — cross-drain boundary scenario missing |
| Eviction | test_eviction01–05 | test_layered34 (frontier gate), test_layered37 (pinned ingest pages), test_layered39 (materialization frontier), test_layered50 (follower clean eviction), test_layered57 (follower app-thread skip) | general eviction tests run via hook | `evict_lru.c`: 5 disagg-specific branches; `evict_page.c`: assert on disagg_info | MOSTLY COVERED — extreme cache pressure + drain concurrency not stress-tested |
| Compaction | test_compact01–16 | none | hook skips all test_compact* | `block_disagg_unsup.c`: compact stubs return 0 silently | KNOWN UNSUPPORTED — API rejection not tested |
| Encryption + compression | test_layered08 (basic read/write), test_layered09, test_layered20, test_layered74 | test_layered09 (delta pages + enc/comp), test_layered20 (32-deep delta chain), test_layered74 (internal delta + rotn + snappy) | general encrypt tests run via hook | `block_disagg_write.c`: compress/encrypt flags forwarded to page log; `block_disagg_mgr.c`: `encrypt_skip` function | MOSTLY COVERED — key rotation and leader→follower decrypt path under load not explicitly tested |
| Statistics (disagg-specific) | none | test_disagg04 (cold put/get stats), test_layered44 (page_discard stat), test_layered04 (btree_entries), test_layered39 (frontier stats), test_layered57 (eviction skip stat) | n/a | `stat.h`/`wiredtiger.h`: 17 disagg connection stats defined | PARTIAL GAP — step_up_time, database_size, abandon_checkpoint, disagg_role, HS block stats untested in Python tests |
| Concurrency (multi-session/thread on layered) | cppsuite tests, format CONFIG.stress | test_layered60 (table create vs checkpoint), test_layered62 (step-down vs checkpoint), test_layered71 (drop vs checkpoint) — all use single writer thread | format CONFIG.disagg has multi-table but no explicit concurrency stress | `evict_lru.c:1885`: btree walk flags race path | PARTIAL GAP — concurrent multi-session insert + checkpoint + drain not tested; no Python-level insert-concurrency stress |
| Large values / overflow pages | general tests assume overflow works | test_layered48 (overflow stat == 0 for 1000-char keys/values) | hook does not address overflow | reconcile: overflow suppression for disagg btrees | COVERED — rejection path tested; but boundary values (values exactly at leaf_key_max/leaf_value_max) not covered |
| Secondary indexes | test_index01–03, cursor tests | none | hook skips all index: creates on layered tables | no index support in disagg | KNOWN UNSUPPORTED — rejection path tested only in hook; no dedicated test asserting error |
| Cursor bounds | test_cursor_bound* | test_layered05 (search_near+bounds), test_layered82 (comprehensive) | hook skips test_cursor_bound* tests | no disagg-specific bound code | MOSTLY COVERED — test_layered82 provides comprehensive bound testing (inclusive/exclusive, all key locations, tombstones, checkpoint mid-iteration, search+bound, reset+bound); remaining gaps are bounds+read_timestamp and bounds+role_transition (CR-H1, CR-H6 in scenario analysis) |
| Rollback to Stable | test_rollback_to_stable01–46 | test_layered05 (RTS reference), test_layered21, test_layered23, test_layered38, test_layered80, test_layered83, test_layered84, test_layered87, test_layered91 (all use RTS in setup/teardown) | hook skips `rollback_to_stable` named tests | `rollback_to_stable/`: no disagg-specific code found | KNOWN UNSUPPORTED (NEVER) — RTS is globally skipped for disagg connections (txn_recover.c:1355-1398, txn.c:2593-2594); the test goal is a negative/behavior test (confirm clean skip or error), not a correctness verification. See RTS-1–RTS-5 in 08_unsupported_features.md |
| Named checkpoints | test_checkpoint* | none | hook skips all `name=` checkpoint calls | no named checkpoint support in disagg | KNOWN UNSUPPORTED — hook skips cleanly; no dedicated rejection test |
| Column-store on layered | many column-store tests | none | hook skips `key_format=r` creates | no column-store in disagg (FIXME-WT-14738) | KNOWN UNSUPPORTED — no rejection test |
| Salvage | test_salvage* | none | hook skips all salvage | `block_disagg_unsup.c`: salvage stubs return ENOTSUP | KNOWN UNSUPPORTED — FIXME-WT-14740 |
| Read-only connections | test_readonly* | none | hook skips `readonly=true` | FIXME-WT-17177 | KNOWN UNSUPPORTED |

---

## Missing Coverage

### [CRITICAL] Backup: No backup path exists or is tested for disaggregated storage

**Feature:** Full and incremental backup

**What is not tested:** Any form of backup on a layered database. The hook skips every `backup:` cursor open and every `test_backup*` test. There is no disagg-specific test that exercises backup semantics. The page log (palite) has no backup API integration.

**Risk:** Production deployments need a backup strategy. If page log data is backed up separately from the checkpoint metadata, a restore could land in an inconsistent state. If the page log is treated as immutable object store, the backup model is entirely different from local-file backup — and this divergence is nowhere tested.

**Disagg-specific code path:**
- Source: `src/cursor/cur_backup.c`
- Path: `cur_backup.c` has zero references to `disagg`, `layered`, or `page_log`. The backup cursor implementation is unaware of disaggregated storage.
- Why tests miss it: `hook_disagg.py:session_open_cursor_replace` calls `skip_test("backup on disagg tables not yet implemented")` on any `backup:` URI. All `test_backup*` tests are skipped by `should_skip`.

**Proposed test:**
- Setup: Create a layered table as leader, insert 1,000 records, checkpoint.
- Operations: Attempt `session.open_cursor("backup:")` and assert `WiredTigerError` is raised with a clear "not supported" message (rather than a silent hang or crash). Verify the error message explicitly mentions disagg/layered storage.
- Assertions: Confirm the error code is `ENOTSUP` or equivalent, and that the connection remains usable afterward (no corrupted state). Document the intended alternative (e.g., "snapshot page log at checkpoint boundary").

---

### [CRITICAL] History Store: Shared HS correctness with deep update chains and concurrent readers

**Feature:** History Store (shared `WiredTigerSharedHS.wt_stable` for disagg)

**What is not tested:** The disagg-specific `hs_conn.c` path creates a *shared* history store (`WT_HS_URI_SHARED = "file:WiredTigerSharedHS.wt_stable"`) that lives in the page log alongside stable btree data. The only disagg HS test is `test_layered25`, which verifies historical reads survive `restart_without_local_files`. None of the 33+ `test_hs*` tests run in disagg mode (the hook does not skip them, but they create plain tables, not layered ones, so the shared HS is not exercised).

**Risk:** The shared HS has different dhandle routing (`session_dhandle.c:433` branches on `__wt_conn_is_disagg`). HS insertion, eviction, reconciliation, and lookup all have separate paths for the shared file. A bug in the shared HS lookup or in its persistence to the page log would silently return wrong historical values or crash on read at an older timestamp.

**Disagg-specific code path:**
- Source: `src/history/hs_conn.c:155–156`, `src/session/session_dhandle.c:433`, `src/cursor/cur_hs.c:164`, `src/btree/bt_handle.c:63–110`
- Path: On `__wt_conn_is_disagg`, `hs_conn.c` creates `WiredTigerSharedHS.wt_stable`; all HS cursor opens route to this file instead of `WiredTigerHS.wt`. The shared file is a disagg btree and its checkpoint is included in the page log alongside user data.
- Why tests miss it: `test_hs*` tests use `SimpleDataSet` → plain `table:` URIs → hook converts to layered, but the HS is used internally only when old versions are evicted to it. The tests do not verify that old versions end up in the shared (not local) HS, nor that the shared HS is correctly replicated to followers.

**Proposed test:**
- Setup: Leader creates a layered table, inserts 5,000 rows at ts=10, pins oldest=1 (long-running reader session), updates all 5,000 rows at ts=20, checkpoints (forces eviction to shared HS).
- Operations: Assert `cache_hs_insert > 0`. Perform `restart_without_local_files(step_up=True)`. On the fresh leader read all rows at ts=10 (must hit shared HS) and ts=20.
- Assertions: Verify ts=10 reads return original values (from shared HS reconstruction), ts=20 reads return updated values. Check `disagg_block_hs_get > 0` and `disagg_block_hs_put > 0` statistics.

---

### [MEDIUM] Rollback to Stable: Behavior test only — RTS is NEVER supported on disagg (RTS-1–RTS-5)

**Feature:** Rollback to Stable

**Classification: NEVER supported on disagg connections.** Recovery RTS is globally skipped
(`txn_recover.c:1355-1398` and `txn.c:2593-2594` short-circuit when `disagg=true`). Explicit
`session.rollback_to_stable()` calls are also unsupported. This is tracked as RTS-1 through RTS-5
in `05_scenario_analysis/08_unsupported_features.md`.

The many `test_layered*` tests that call `rollback_to_stable` in their setup/teardown succeed only
because those calls run on non-disagg connections used for setup scaffolding — not on the layered
connection itself. The `format CONFIG.disagg` profile RTS call is also skipped at the global level.

**Test goal (behavior test, not correctness verification):**
- Confirm that calling `session.rollback_to_stable()` on a disagg connection either returns a
  clean error or is safely skipped — without corrupting data or crashing.
- Verify that `txn_rts_btrees_applied == 0` (no btrees were walked).
- Confirm the global skip paths in `txn_recover.c:1355-1398` and `txn.c:2593-2594` are hit.

**Why the original PARTIAL GAP assessment was incorrect:**
RTS is not a correctness gap; it is an unsupported feature. The disagg-global skip fires before
any btree-level RTS code runs, so there is no "RTS on stable btree page" correctness concern to
verify. The `WT_UPDATE_DURABLE` flag in `rts_btree.c:792` is dead code from the perspective of
production disagg workloads.

---

### [HIGH] Concurrent multi-session insert stress: No parallel writer tests for layered tables

**Feature:** Concurrency (multiple sessions writing to layered tables simultaneously)

**What is not tested:** Concurrent insert/update workloads from multiple threads targeting the same layered table. All `test_layered*` Python tests use a single writer session. The threading present in `test_layered60`, `test_layered62`, `test_layered68`, `test_layered71` is always a checkpoint thread vs. a schema operation — never concurrent data writers. The only multi-writer disagg stress is in `format CONFIG.disagg`, which has 3 tables and leader/follower topology but its output is not independently verified at the Python test level.

**Risk:** The ingest btree accumulates all in-flight writes and must be drained by the layered table manager. Concurrent writers create concurrent update chains on the ingest btree pages. The `evict_lru.c:1885` path that re-evaluates btree walk flags on disagg connections is sensitive to concurrent flag updates. A race between concurrent ingest and checkpoint drain could corrupt the ingest btree or produce duplicate keys in the stable btree.

**Disagg-specific code path:**
- Source: `src/conn/conn_layered_ingest.c`, `src/evict/evict_lru.c:1885`, `src/txn/txn_truncate.c` (layered truncate list with a read/write lock)
- Path: Multiple sessions writing to the ingest btree share the `truncate_lock` and the ingest btree dhandle. The eviction server walks btrees with `last_evict_walk_flags` checked per-btree in a disagg-specific branch. Concurrent writers can expose races in these structures.
- Why tests miss it: Python tests are inherently single-threaded (one writer session). Format covers this but its assertions are statistical (no per-key correctness verification).

**Proposed test:**
- Setup: 4–8 Python threads, each with its own session on a single layered table, inserting/updating disjoint key ranges simultaneously for 30 seconds.
- Operations: Interleave checkpoints from a background thread. After the stress period, quiesce all writers and do a full scan.
- Assertions: Total key count matches expected; no duplicate keys; `btree_entries` stat equals the scan count. Check that no sessions returned `WT_ROLLBACK` more than expected.

---

### [HIGH] Compaction: No test verifying error behavior when compaction is attempted on layered tables

**Feature:** Compaction (foreground and background)

**What is not tested:** Whether `session.compact("layered:foo")` returns a proper error or silently does nothing. The `block_disagg_unsup.c` stubs for `compact_start`, `compact_end`, `compact_skip`, and `compact_page_skip` all return 0 (success) without any work. The hook skips all `test_compact*` tests. There is no test verifying the behavior from the caller's perspective.

**Risk:** If a caller invokes compaction on a disagg table expecting it to reclaim space and the stub silently succeeds with no work done, the caller has no indication that compaction is a no-op. If the caller relies on compaction to reduce space usage (as some MongoDB maintenance paths do), this silent no-op becomes a space leak. Additionally, the `background_compact` subsystem is disabled in `CONFIG.disagg` but the hook does not explicitly skip `test_compact` for background compaction separately from foreground.

**Disagg-specific code path:**
- Source: `src/block_disagg/block_disagg_unsup.c:36–86`
- Path: `__wti_block_disagg_compact_start`, `__wti_block_disagg_compact_skip`, `__wti_block_disagg_compact_page_skip`, `__wti_block_disagg_compact_end` — all stubs that return 0 without setting `*skipp` or doing any work.
- Why tests miss it: `hook_disagg.py:should_skip` matches `"test_compact"` and skips. `session_compact_replace` forwards to the original compact (which hits the stub silently).

**Proposed test:**
- Setup: Create a layered table, insert 10,000 rows, delete 9,000 rows, checkpoint.
- Operations: Call `session.compact("layered:foo")`. Verify the call returns without error. Read statistics: `btree_compact_pages_reviewed == 0` (no pages reviewed, confirming it is a no-op).
- Assertions: Confirm the no-op behavior is deterministic, not accidental. Add a test that `background_compact` also gracefully does nothing on a disagg table. Document explicitly in a comment that this is intended.

---

### [MEDIUM] Statistics: Key disagg connection statistics have no Python-level assertions

**Feature:** Disagg-specific statistics

**What is not tested:** Of the 17 disagg-specific connection statistics defined in `wiredtiger.h` (IDs 1049–1560), only 4 are checked by Python tests: `disagg_block_put_cold` (test_disagg04), `disagg_block_get_cold` (test_disagg04), `disagg_block_page_discard` (test_layered44), and frontier-related eviction stats (test_layered39). The following are never asserted by any Python test:
- `WT_STAT_CONN_DISAGG_STEP_UP_TIME` (only checked in the C++ perf test)
- `WT_STAT_CONN_DISAGG_STEP_DOWN_TIME`
- `WT_STAT_CONN_DISAGG_DATABASE_SIZE`
- `WT_STAT_CONN_DISAGG_ROLE_LEADER`
- `WT_STAT_CONN_DISAGG_ABANDON_CHECKPOINT_FAILED` / `_SUCCEED`
- `WT_STAT_CONN_DISAGG_BLOCK_HS_BYTE_READ` / `_WRITE` / `_GET` / `_PUT` (shared HS I/O tracking)

**Risk:** Untested statistics decay silently — code changes may break stat increments without detection. The shared HS byte stats are particularly important for monitoring history store I/O in production.

**Disagg-specific code path:**
- Source: `src/include/stat.h:767`, `build/include/wiredtiger.h:1049–1560`
- Path: Each stat is incremented by a specific code path (e.g., `disagg_step_up_time` by `conn_layered.c` on role reconfigure, `disagg_database_size` by `checkpoint_txn.c:849–899`). None of these incrementing paths are covered by a Python assertion.
- Why tests miss it: No test was written for them. The C++ perf test only checks `DISAGG_STEP_UP_TIME` as a performance metric, not a correctness assertion.

**Proposed test:**
- Setup: Leader inserts data, checkpoints, then closes. Follower opens, picks up checkpoint, steps up to leader.
- Operations: Read connection stats after step-up.
- Assertions: `disagg_step_up_time > 0`, `disagg_role_leader == 1`, `disagg_database_size > 0`. Insert more data, checkpoint, and assert `disagg_database_size` increases. Trigger an `abandon_checkpoint` condition and assert `disagg_abandon_checkpoint_succeed > 0`.

---

### [MEDIUM] Prepared transactions: Prepared txns crossing drain boundaries not tested

**Feature:** Prepared transactions — cross-drain boundary behavior

**What is not tested:** A prepared transaction that is active *while the ingest table is being drained to the stable btree*. `test_layered45` tests delta skip for prepared updates. `test_layered65` tests GC of prepared updates in the ingest table. `test_layered94` tests step-up with a prepared txn active. But none test the scenario where a drain operation (ingest → stable) is triggered concurrently with a prepared transaction on the *same key* — specifically, whether the ingest table drain correctly preserves the prepared update visibility constraints and does not expose it prematurely in the stable btree.

**Risk:** If the drain operation does not correctly skip prepared updates (i.e., it treats them as committed), the stable btree could contain data that a follower reads as committed even though the prepare has not resolved. This would violate prepared transaction isolation guarantees across the disagg topology.

**Disagg-specific code path:**
- Source: `src/reconcile/rec_visibility.c:153–245` — the `is_disagg` flag gate in visibility resolution. `src/conn/conn_layered_ingest.c` — the drain trigger.
- Path: `rec_visibility.c:200,245` — when `is_disagg` is true, prepared updates are handled differently (they may be written with `preserve_prepared` semantics). A concurrent drain could race with a prepare that is not yet committed.
- Why tests miss it: `test_layered45` uses single-session sequential ops. The drain happens implicitly at checkpoint. No test forces a drain concurrently with a live prepared transaction on the same page.

**Proposed test:**
- Setup: Insert 1,000 rows, checkpoint (base stable btree). In session A, prepare a transaction updating key 500 (do not commit yet).
- Operations: In a background thread, force a checkpoint (which triggers drain). While checkpoint is running, commit the prepared transaction in session A.
- Assertions: After checkpoint, the stable btree shows the committed value for key 500. A follower picking up the checkpoint also sees the correct value. No crash or assertion failure in `rec_visibility.c`.

---

### [MEDIUM] Encryption: Key rotation path in disagg context not tested

**Feature:** Encryption — key rotation via key provider extension

**What is not tested:** The key rotation flow specific to disaggregated storage. `block_disagg_ckpt.c:181` has code to gather updated key encryption information and write it into the shared checkpoint metadata at each checkpoint. `hook_disagg.py` loads a `key_provider` extension when configured, and `test_layered28.py` is the only layered test using `key_provider` — but its analysis shows it tests table drop semantics, not encryption key rotation. No test verifies: (a) that re-encryption across a key rotation is durable in the page log, (b) that a follower can read data encrypted with a rotated key, (c) that the shared checkpoint metadata correctly carries the new key reference.

**Risk:** If a key rotation produces checkpoint metadata that a follower cannot decrypt, the follower's step-up fails silently or the data appears corrupted. In a production deployment with automated key rotation, this would cause follower failover to fail.

**Disagg-specific code path:**
- Source: `src/block_disagg/block_disagg_ckpt.c:181`
- Path: "Gather any updated key encryption information so it can be written into the shared metadata" — this path runs during checkpoint and is entirely untested at the Python level.
- Why tests miss it: `test_layered08` tests encryption but not key rotation. `test_layered28` uses `key_provider` but for unrelated reasons.

**Proposed test:**
- Setup: Open with `key_provider` extension and `rotn` encryptor. Insert 100 rows, checkpoint.
- Operations: Trigger a key rotation (advance the key provider epoch). Insert 100 more rows, checkpoint.
- Assertions: Follower opens, picks up the post-rotation checkpoint, reads all 200 rows correctly. Verify the shared checkpoint metadata contains the rotated key reference.

---

### [LOW] Secondary indexes: Rejection path not independently tested

**Feature:** Secondary indexes on layered tables

**What is not tested:** A dedicated test asserting that `session.create("index:layered_foo:idx_bar", ...)` raises a specific `WiredTigerError`. The hook silently skips these creates, so the test never reaches the WT API. Any test using indexes is skipped before WiredTiger sees the call.

**Risk:** Low — the hook works correctly. But if the hook is removed or a user calls the index API directly from C, there is no test verifying a clean error. The behavior may be a crash rather than a proper error.

**Disagg-specific code path:**
- Source: `src/schema/schema_create.c` — no disagg-specific index rejection code was found
- Path: `hook_disagg.py:session_create_replace:311–313` — Python-level skip only. The underlying C code may crash or corrupt state if `index:` is created on a layered base table.
- Why tests miss it: Hook intercepts before the API is called.

**Proposed test:**
- Setup: Create a layered table.
- Operations: Attempt `session.create("index:layered_foo:idx_bar", "columns=(v)")` without the hook.
- Assertions: Verify `WiredTigerError` is returned with a clear "not supported" message.

---

## Summary: Top Missing Features Ordered by Risk

1. **[CRITICAL] Backup** — No backup API or strategy for disagg; `backup:` cursor is entirely unimplemented and untested. Any backup recovery scenario is unknown.

2. **[CRITICAL] History Store correctness** — The shared HS (`WiredTigerSharedHS.wt_stable`) is a new disagg-specific entity. Its write/read/eviction paths are covered only by a single incidental test (`test_layered25`). The 33 dedicated `test_hs*` tests do not exercise the shared HS. Bugs here silently produce wrong historical reads.

3. **[HIGH] Concurrent multi-session insert stress** — No Python-level test runs parallel writers on a layered table. The disagg-specific eviction walk flag race (`evict_lru.c:1885`) and the ingest truncate list lock (`txn_truncate.c`) are never stressed by concurrent sessions.

4. **[HIGH] Compaction behavior** — All compact stubs silently return 0. No test verifies the caller receives any indication that compaction is a no-op. Background compaction is disabled in `CONFIG.disagg` but the disabling is not itself tested.

5. **[MEDIUM] Disagg statistics coverage** — 13 of the 17 disagg-specific connection statistics are never asserted in Python tests. These include `step_up_time`, `database_size`, `role_leader`, `abandon_checkpoint_*`, and all shared HS I/O stats.

6. **[MEDIUM] Prepared transactions across drain boundary** — The single most dangerous prepare+drain race (prepared update on a page being drained to stable btree concurrently) is not tested.

7. **[MEDIUM] Key rotation with encryption** — The `block_disagg_ckpt.c` key-rotation metadata path is never covered by any test.

8. **[MEDIUM] Rollback to Stable** — RTS is NEVER supported on disagg; the test goal is a negative/behavior test (confirm clean skip or error). See RTS-1–RTS-5 in `05_scenario_analysis/08_unsupported_features.md`.

9. **[LOW] Secondary index rejection** — No C-level test for clean error on index creation. Hook masks the issue at the Python layer.
