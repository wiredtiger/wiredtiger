# Gap Analysis: Non-DisAgg Tickets + FIXME Scan
Generated: 2026-05-06

---

## Section A: FIXMEs in Source Code (Testing Relevant)

### FIXME: History store validation disabled in bt_vrfy.c
**Location:** `src/btree/bt_vrfy.c:1267`
**Comment:** `/* FIXME-WT-10779 - Enable the history store validation. */`
**Testing implication:** The verify path (`wt verify`) skips history store consistency checks. Corruptions in the HS that would be caught by a full verify pass are silently missed. No test exercises verify with HS validation enabled.
**Suggested test:** Enable the validation on a test database with deliberate HS content (prepared transactions, multiple versions) and confirm verify reports no false positives; then inject an HS corruption and confirm verify catches it.

### FIXME: Synchronization around oldest_timestamp/stable_timestamp unchecked
**Location:** `src/txn/txn_timestamp.c:548`
**Comment:** `/* FIXME-WT-16310: Check synchronization around oldest_timestamp and stable_timestamp. */`
**Testing implication:** A known potential data race on global timestamp fields has no test exercising the concurrent update path under TSAN. Related to WT-16717 (stable_timestamp TSAN warning).
**Suggested test:** TSAN build stress test that concurrently advances oldest/stable timestamps while running transactions with prepared commits, to expose the unsynchronized read.

### FIXME: Prepared fast truncate not handled in prepared_discover_walk
**Location:** `src/prepared_discover/prepared_discover_walk.c:328`
**Comment:** `/* TODO: handle prepared fast delete. */`
**Testing implication:** The prepared transaction discovery walk used during step-up does not account for fast-truncated pages. After a failover, prepared fast truncates on the old leader may be silently ignored during reconstruct.
**Suggested test:** WT-17277 tracks adding prepared fast truncate to test_checkpoint and test_format — this FIXME confirms the gap is real. A dedicated recovery test exercising step-up with a prepared fast truncate outstanding would be the direct regression test.

### FIXME: bt_sync_obsolete does not read non-logged internal pages
**Location:** `src/btree/bt_sync_obsolete.c:453`
**Comment:** `/* FIXME: Read internal pages from non-logged tables when the remove/truncate */`
**Testing implication:** Obsolete content on non-logged internal pages is not cleaned up by the sweep-during-sync path. Long-running tests that use non-logged tables with heavy truncate/remove might accumulate obsolete pages that are never reclaimed.
**Suggested test:** cppsuite stress test combining non-logged tables with high truncate frequency and a stat check that obsolete page count does not grow unboundedly.

### FIXME: Verify setup for tiered tables incomplete
**Location:** `src/block/block_ckpt.c:120`
**Comment:** `/* FIXME: We may need to change how we setup for verify when it supports tiered tables. */`
**Testing implication:** The verify path for tiered (and by extension layered/disagg) tables has not been fully designed. Existing `wt verify` tests skip tiered/disagg configurations.
**Suggested test:** Once verify is supported for layered tables (per roadmap), a catch2 unit test for `block_ckpt.c` verify setup with a multi-object tiered btree would close this gap.

### FIXME: Recovery cleanup of incomplete complex/tiered tables missing
**Location:** `src/txn/txn_recover.c:902`
**Comment:** `/* FIXME-WT-16146: Add capability for cleaning up incomplete complex tables. */`
**Testing implication:** After a crash during creation of a complex table (table with index or column group), incomplete metadata can be left in place. Recovery does not clean it up. No crash test exercises this scenario.
**Suggested test:** csuite crash test: create a table+index, crash mid-create, recover, verify metadata is clean and the table is either fully present or fully absent.

### FIXME: Checkpoint cookie buffer can overflow when using large address cookies
**Location:** `src/block/block_addr.c:111` (and WT-15022 description)
**Comment:** `/* TODO: testing has-objects is not quite right. */` (block_addr.c:111); separately, WT-15022 identifies that `WT_BLOCK_CHECKPOINT_BUFFER` is not sized relative to `WT_ADDR_MAX_COOKIE`, allowing overflow if disagg uses larger address cookies.
**Testing implication:** No test exercises address cookies near the `WT_ADDR_MAX_COOKIE` limit. The disagg block manager uses larger cookies and could silently overflow the checkpoint buffer.
**Suggested test:** A catch2 unit test that packs a checkpoint cookie with the maximum-sized address cookies (using disagg format) and asserts no buffer overflow occurs; also test the graceful failure path.

### FIXME: meta_tracking not initialized during recovery (schema_drop path)
**Location:** `src/schema/schema_drop.c:542`
**Comment:** `/* FIXME-WT-16215: During recovery (including partial backup restore), the meta tracking has not been initialized */`
**Testing implication:** The drop path during recovery takes a different code path (no meta tracking) that differs from the normal path. Error injection into this path is untested.
**Suggested test:** csuite test that drops a table and then performs a partial backup restore, recovering through the drop.

---

## Section B: Non-DisAgg Tickets with Testing Gaps

### Confirmed Gaps

#### [WT-14029] — Add timing stress config to live restore
**Gap:** Live restore completes too quickly on small test files to exercise concurrent workload races. The timing stress hook `WT_TIMING_STRESS_LR_SLOW` does not yet exist. Existing live restore tests (test_live_restore01–06) test functional correctness but not concurrent migration races.
**Suggested test:** Implement `WT_TIMING_STRESS_LR_SLOW` in `__wti_live_restore_fs_restore_file`, then enable it in existing live restore Python tests when run under timing stress mode. This enables race detection between file migration and concurrent I/O under TSAN.

#### [WT-14395] — Crash during a checkpoint should not advance the oldest timestamp
**Gap:** Labeled `model-test`. A crash at the final checkpoint crash point (after txn commit, before turtle file update) produces different oldest timestamp results depending on whether logging is enabled. The model does not capture the logging-enabled variant of this scenario.
**Suggested test:** Extend test/model's `checkpoint_crash` workload to parameterize over `log=(enabled=true/false)` and assert that oldest_timestamp after recovery equals 50, not 100, in both cases.

#### [WT-14688] — Improve live restore server test coverage
**Gap:** Only a single MongoDB server test (`live_restore_sharded_backup_restore.js`) exercises live restore, covering only a single-node scenario to avoid race conditions. Multi-node interaction (e.g., live restore + oplog truncation, live restore + stepdown) is not tested.
**Suggested test:** Design and implement a multi-node live restore server test (tracked in MongoDB SERVER repo per the ticket); on the WT side, add a cppsuite test that runs live restore concurrently with checkpoint and oplog-like truncation operations.

#### [WT-15022] — Size of checkpoint cookie should be defined in terms of size of address cookie
**Gap:** `WT_BLOCK_CHECKPOINT_BUFFER` is independently sized (127 bytes) rather than as a multiple of `WT_ADDR_MAX_COOKIE` (255 bytes). The disagg block manager uses larger cookies and can overflow this buffer when more than ~3 extra 64-bit integers are added to the address cookie. No test validates the buffer limit.
**Suggested test:** catch2 unit test in `block_ckpt.c` that constructs a maximum-sized disagg address cookie and calls the pack/unpack checkpoint cookie functions, asserting no overflow. Also add an assertion in the C code that fires before a buffer overflow occurs.

#### [WT-15061] — Add crash point before checkpoint txn commit
**Gap:** Existing checkpoint crash points are at: (1) before metadata updates, (2) before metadata sync. There is no crash point before the checkpoint transaction commit itself. This means the recovery path from a crash during the commit is not exercised by deterministic crash testing.
**Suggested test:** Add `WT_TIMING_STRESS_CHECKPOINT_CRASH_BEFORE_TXN_COMMIT` crash point in `checkpoint_txn.c` at line ~1485, then add it to the model test's `checkpoint_crash` workload.

#### [WT-15243] — Bulk cursor and drop segmentation fault
**Gap:** When a bulk cursor has `WT_DHANDLE_EXCLUSIVE` set on a handle, calling `drop()` from the same session incorrectly succeeds (returns 0) instead of returning EBUSY, then segfaults during checkpoint-tree. No test exists that opens a bulk cursor and then attempts `drop()` on the same table.
**Suggested test:** Python test: open a bulk cursor on a table, do not close it, attempt `session.drop()` on that table URI, and assert EBUSY is returned. This directly covers the error path at `cur_file.c:1091` that WT-16421 also targets.

#### [WT-15312] — WT_SESSION::drop can incorrectly return EBUSY due to WT_UNCOMMITTED_DATA
**Gap:** Even after all application transactions have committed, `drop()` can return EBUSY with `WT_UNCOMMITTED_DATA` sub-error. The cause is not fully understood. MongoDB has an invariant commented out (SERVER-100890) because of this. No WT-side regression test reproduces the scenario.
**Suggested test:** Python test: write committed data to a table, then call `drop()` repeatedly, asserting that after N retries (say 5) it succeeds. Optionally assert it never returns infinite EBUSY.

#### [WT-16421] — Create a test to cover invalid path: checkpoint cursors + bulk cursors
**Gap:** The error path at `cur_file.c:1091` ("checkpoints are read-only and cannot be bulk-loaded") is unreachable in any existing test. The error fires when a bulk cursor is opened on a table that already has a checkpoint cursor active.
**Suggested test:** Python test that opens a checkpoint cursor (`checkpoint=WiredTigerCheckpoint`), then attempts to open a bulk cursor (`bulk=true`) on the same table URI, and asserts `EINVAL` is returned.

#### [WT-16713] — Create in-WiredTiger tests for victim block cache
**Gap:** The victim block cache (page eviction cache used in disagg, exposed via `WT_PAGE_LOG_HANDLE` API) has no functional test within the WT repository itself. All block cache testing is either in MongoDB or in the disabled `test_layered43.py` (which calls `skipTest("FIXME-WT-15663: currently block cache is disabled.")`).
**Suggested test:** Implement a mock `WT_PAGE_LOG_HANDLE` in PALite (using `std::unordered_map` + `std::mutex`) and enable it via a config flag. Then enable block cache in test/format and the layered Python suite to provide basic functional coverage of the victim eviction code path.

#### [WT-16834] — Add regression test for table IDs conflict
**Gap:** Table ID conflict between the key provider table and the shared metadata was caught by MongoDB CI (BF-41795, BF-41785) but not by WT's own tests. There is no WT test that deliberately creates tables in a sequence that exercises the ID-assignment boundary.
**Suggested test:** Python or catch2 test that creates a large number of internal and user tables (potentially with drops and recreates to reuse IDs), verifies no ID is assigned twice, and confirms correct isolation of key-provider table IDs from regular table IDs.

#### [WT-16836] — Investigate ways for comprehensive testing for Table IDs conflicts
**Gap:** Companion to WT-16834. Proposes detecting duplicate table ID assignment at the PALI layer by maintaining a set of allocated IDs. Currently PALite's `pl_open_handle` conflates create and find, making it hard to instrument. No detection mechanism exists in either PALite or MongoDB's PALI implementation.
**Suggested test:** Instrument PALite to maintain a `std::unordered_set<uint64_t>` of allocated table IDs and assert uniqueness on each `pl_open_handle` call during tests.

#### [WT-16923] — Test coverage for dirty bytes stat in checkpoint progress messages
**Gap:** WT-16912 added a stat that prints dirty bytes per btree in checkpoint progress messages. No test verifies that this stat decrements correctly during checkpoint (non-parallel write case) or accumulates to match bytes_written.
**Suggested test:** Python test: write a fixed amount of data, trigger a single checkpoint, read the checkpoint progress messages from the WT verbose log, and assert dirty_bytes at start of checkpoint >= bytes_written_by_checkpoint. Use `wttest.captureout()` or WT verbose output parsing.

#### [WT-16983] — Assertion failure hit on checkpoint size
**Gap:** An assertion `WT_ASSERT(session, ckpt->size == btree->bytes_total)` fires in the disagg checkpoint path when a step-down followed by step-up leaves inconsistent metadata (size=1903, addr=empty cookie). This was found while writing a regression test for WT-16974. No stable reproducer or regression test exists yet.
**Suggested test:** Disagg-specific test: step-down, step-up, immediately run a checkpoint, and assert no assertion failure. The reproducer attached to the ticket is a starting point.

#### [WT-17277] — Add testing support for prepared fast truncate in test_checkpoint and test_format
**Gap:** Neither `test_checkpoint` nor `test_format` generates prepared truncations. The entire prepared fast truncate code path (write-to-disk, claim on restart, rollback-to-stable, crash recovery) is exercised only by hand-written unit tests, not by any randomized stress tester.
**Suggested test:** (Per the ticket's own acceptance criteria) Extend `test_format` to generate prepared truncation ops when prepare is enabled; extend `test_checkpoint` to exercise prepared fast truncate under concurrent checkpoint. Both should verify correctness after recovery and RTS.

#### [WT-17381] — TSAN data race in __wt_delete_page_rollback writing to instantiated tombstone
**Gap:** A real TSAN data race exists between `__wt_delete_page_rollback` and a concurrent cursor reader on instantiated tombstone `WT_UPDATE` fields. The race is nondeterministic and was found via test/format with a TSAN build. The unconditional write to `upd_saved_txnid` at `bt_delete.c:302` clobbers `upd_start_ts` for non-prepared rollbacks.
**Suggested test:** The existing test/format TSAN reproducer (`-c ../../../test/format/CONFIG.stress -T bulk,txn,retain=50 runs.rows=100000:300000 runs.tables=1:3 runs.ops=300000`) should be added as a named CI test configuration. Once fixed, add a TSAN-enabled targeted concurrent test (open cursor + rollback prepared delete page in parallel).

#### [WT-15084] — Run test/model with logging enabled on tables
**Gap:** test/model always runs tables without WAL logging. Logged collections (like the MongoDB oplog) have different recovery semantics. test/model does not simulate the case where some tables are logged and others are not — a configuration used in practice by mongod.
**Suggested test:** Add probabilistic `log=(enabled=true)` configuration to a subset of tables in test/model's workload generator. Assert that after crash+recovery, logged tables are consistent with WAL and non-logged tables are consistent with their last checkpoint.

#### [WT-17181] — Ensure compatibility test coverage for minor releases is on par with major releases
**Gap:** Starting with MongoDB 8.2, minor releases are production-quality and require the same upgrade/downgrade testing as major releases. Current compatibility tests do not cover minor release ↔ adjacent minor release paths (e.g., 8.2 ↔ 8.1, 8.2 ↔ 8.3).
**Suggested test:** Audit compatibility test matrix; add version pairs `(8.1, 8.2)`, `(8.2, 8.3)` etc. in the same fashion as existing `(8.0, 9.0)` pairs. Scheduled to be addressed in Sprint `SE Foundations - 2026-05-22`.

### Uncertain Cases (potential gaps, insufficient detail to confirm)

#### [WT-14037] — Eviction gets stuck due to server enqueuing non-evictable pages
**Reason:** The bug (eviction server calling `__wt_evict_page_urgent` on internal pages without first calling `__wt_page_can_evict`) has an open PR. The PR shows test results. It is unclear whether the fix includes a regression test for the "enqueue non-evictable page" scenario. Needs confirmation that a targeted eviction unit test was added.

#### [WT-14031] — op_timer_fired mechanism doesn't free threads stuck in eviction after commit/rollback
**Reason:** The issue was de-prioritized (MongoDB moved from `operation_timeout_ms` to `cache_max_wait_ms`). No regression test was added. Low priority since the workaround is in place, but the underlying bug is unfixed and untested.

#### [WT-16672] — Investigate if prefetch should be tested in test/model
**Reason:** Under investigation (assigned to Dylan Liang). Peter Macko confirmed enabling prefetch in test/model is feasible and would not break determinism. The decision of whether to implement it has been made in favor of trying it, but implementation is not started.

#### [WT-14564] — Investigate WiredTiger metadata corruption detected error during log recovery
**Reason:** Open investigation ticket. The scenario (metadata corruption detected during log recovery) could benefit from a focused crash test, but the root cause is not yet understood.

#### [WT-16022] — Segfault when importing a table
**Reason:** Open bug. The import path has limited test coverage (test_import11 has verification disabled for tiered storage). No dedicated segfault regression test exists.

#### [WT-16905] — WT hangs on implicit read-uncommitted search after modify update
**Reason:** Open bug. No repro script visible in the ticket. If confirmed, needs a targeted test for the implicit read-uncommitted path after `modify`.

### Explicitly Test-Creating Tickets (already have a test plan)

- **WT-17216**: Bulk insert flush — add functional test suite (Python suite). Planned.
- **WT-17217**: Bulk insert flush — add crash recovery tests. Planned.
- **WT-17218**: Bulk insert flush — cppsuite stress: concurrent bulk loads with background checkpoint. Planned.
- **WT-17219**: Bulk insert flush — cppsuite stress: crash injection during flush. Planned.
- **WT-17220**: Bulk insert flush — cppsuite stress: memory pressure. Planned.
- **WT-17221**: Bulk insert flush — scale test. Planned.
- **WT-17251**: Add logging to test/format predictable replay (diagnostic aid). In progress (Alex Pullen).
- **WT-17260**: Support ops on newly inserted keys in predictable replay. PR merged.
- **WT-17294**: Enable pre-positioning cursor in predictable replay. Backlog (placeholder only, moved to backlog by Ivan Kochin).
- **WT-16615**: Add wt_binary_decode tests to Evergreen. Open, assigned.
- **WT-14098**: Re-enable python test checkpoint33 in TSan testing. Backlog.
- **WT-14335**: Fix test_syscall on MacOS and enable in CI. Backlog.
- **WT-14374**: Reorganize deterministic/predictable tests in Evergreen. Open.
- **WT-16937**: Enable debug_mode.cursor_copy in WT ASAN/MSAN/UBSAN testing. Open.

---

## Section C: Disabled/Skipped Tests Requiring Attention

### Critical: Block cache test disabled indefinitely
- **File:** `test/suite/test_layered43.py:58`
- **FIXME:** `FIXME-WT-15663: currently block cache is disabled.`
- **Source:** `src/block_cache/block_cache.c:874` `/* FIXME-WT-15663 Disable block cache until it is stable. */`
- **Impact:** The entire victim block cache code path is untested in CI. WT-16713 is the ticket to fix this.

### High: Prepared modify reconstruction not verified in test_prepare34.py
- **File:** `test/suite/test_prepare34.py:35`
- **FIXME:** `# FIXME: Verify that prepared modifies are reconstructed properly when loaded from disk`
- **Impact:** The on-disk representation of prepared modifies is not validated after crash+recovery. This is particularly relevant for disagg's "preserve prepared" feature.

### High: test_corrupt01.py entirely disabled for DisAgg
- **File:** `test/suite/test_corrupt01.py:38-39`
- **FIXMEs:** `FIXME-WT-15064: This test is disabled until we have a way to implement corruption tests for DisAgg.`
- **Impact:** All WT corruption detection tests are skipped under the disagg hook. There is no corruption testing for layered tables.

### High: test_sweep04 and test_truncate23 have hard skips without tracking
- **File:** `test/suite/test_sweep04.py:113-114` (`FIXME-WT-13706`, full hard skip)
- **File:** `test/suite/test_truncate23.py:127` (`FIXME-WT-13232`, full hard skip)
- **Impact:** Both tests are completely skipped in all configurations, not just disagg. The original bugs they were written to catch may be fixed but the tests never re-enabled.

### Medium: test_stat10.py disabled (WT-16633)
- **File:** `test/suite/test_stat10.py:108`
- **FIXME:** `FIXME-WT-16633: Re-enable the test once fixed`
- **Impact:** Stat correctness test disabled. If the underlying bug was fixed, the test should be re-enabled.

### Medium: test_jsondump02 and test_cursor_bound16 JSON cursor bug
- **Files:** `test/suite/test_jsondump02.py:91,330` and `test/suite/test_cursor_bound16.py:49`
- **FIXME:** `FIXME-WT-9986: Re-enable after fixing the JSON cursor bug`
- **Impact:** JSON dump output is not verified for column store cursor bounds and structured dump scenarios.

### Medium: test_layered27 step-down scenarios disabled (WT-15763)
- **File:** `test/suite/test_layered27.py:91,150,270`
- **FIXME:** `FIXME-WT-15763: Re-enable once we can abandon changes after stepping down.`
- **Impact:** Three test scenarios for step-down behavior are skipped. Step-down correctness is a core disagg requirement for Public Preview.

### Medium: test_layered88 read-only connection validation missing (WT-17177)
- **Files:** `test/suite/test_layered88.py:51`, `test/suite/test_util23.py:34`
- **FIXME:** `FIXME-WT-17177: Opening a read-only connection with disagg must be rejected.`
- **Impact:** Read-only connections are not validated to return an error in disagg mode. This is an unsupported feature per the unsupported_disagg.md spec.

### Low: test_layered91 cache-stuck abort limits test cases (WT-17160)
- **File:** `test/suite/test_layered91.py:148`
- **FIXME:** `FIXME-WT-17160: Increasing the number of situations results in abort due to cache stuck.`
- **Impact:** A disagg crash/recovery test is limited in its scenario count due to a cache-stuck abort. The abort is itself a test gap — cache pressure management under disagg is not robust enough to run longer tests.

---

## Section D: Observations on test_prepare34.py Gap

The file `test/suite/test_prepare34.py` contains a FIXME at the top level (not inside a skip guard):

```python
# FIXME: Verify that prepared modifies are reconstructed properly when loaded from disk
```

This is a TODO for the test author, not a skip. The test exercises prepared transactions but explicitly documents that it does not verify the disk-reconstruction path for `modify` operations. Since DisAgg's "preserve prepared" feature (WT-17274, WT-17275) will write prepared fast truncates to disk, the same gap applies to the more general "prepared update on disk" scenario for modifies.

---

## Major New Non-DisAgg Testing Areas Identified

1. **Prepared Fast Truncate end-to-end testing** (WT-17277, WT-17274, WT-17275, WT-17276): The entire prepared fast truncate feature set (write to disk, claim on restart, RTS rollback, crash recovery) has no stress test coverage. This is the highest-priority new test area, tracked in a family of 6 tickets (WT-17274–WT-17277, WT-17274–17276).

2. **Bulk Insert Flush testing** (WT-17216–WT-17222): A new bulk cursor flush feature has a complete planned test suite in 7 tickets, none implemented yet. The full suite — Python functional, crash recovery, cppsuite stress, scale, performance — needs to be built.

3. **Victim Block Cache in-WT testing** (WT-16713): The block cache (victim eviction in disagg) has zero in-WT test coverage. Creating a mock PALite implementation and enabling it in test/format is the key deliverable.

4. **Compatibility testing for minor releases** (WT-17181): Minor releases now require the same upgrade/downgrade test coverage as major releases. The test matrix needs to be extended to cover adjacent minor release pairs.

5. **Table ID uniqueness enforcement** (WT-16834, WT-16836): Two postmortem tickets for a production bug caught only by MongoDB CI. WT needs a mechanism to detect duplicate table ID assignment in PALite and a regression test that reproduces the key-provider vs shared-metadata ID conflict.

6. **Checkpoint crash point coverage** (WT-15061): The crash point family is missing the "before checkpoint txn commit" scenario. Adding it would complete the deterministic crash testing of the checkpoint commit pipeline.

7. **test/model with WAL-logged tables** (WT-15084): model currently never tests logged tables. Logged+non-logged table mixed workloads are the real mongod configuration and should be tested in model.

8. **TSAN: delete_page_rollback concurrent access** (WT-17381): An active TSAN race on the instantiated tombstone path needs a regression test. Fix direction is clear (guard the `upd_saved_txnid` write), but no test enforces the fixed behavior.
