# Gap Analysis: Disaggregated Block Layer (src/block_disagg/)

*Coverage analyzed against: test_disagg01-04, test_disagg_checkpoint_size01-04, catch2 misc_tests (test_disagg_meta_config, test_page_log_handle, ext_test_checkpoint_meta_version)*

*Source files analyzed: block_disagg_mgr.c, block_disagg_ckpt.c, block_disagg_read.c, block_disagg_write.c, block_disagg_open.c, block_disagg_addr.c, block_disagg_unsup.c*

---

## Current Coverage Summary

The existing tests collectively exercise the following paths in `src/block_disagg/`:

**block_disagg_mgr.c**
- `__wt_block_disagg_manager_open` — exercised by every test that creates a disaggregated table.
- `__bmd_close` — exercised on teardown.
- `__bmd_free` (via eviction in test_disagg_checkpoint_size03) — exercised in `test_size_leak_after_rec_result_page_clean` and `test_cumulative_size_leak_after_eviction`, but only with `is_root=false`. The `is_root=true` path (old root page discard on checkpoint) is implicitly touched by every checkpoint test but its size-accounting invariant is not explicitly verified.
- `__bmd_write` / `__bmd_stat` — exercised by all data-writing tests.
- `__bmd_get_page_ids` — the `plh_get_page_ids == NULL` warning branch is not tested; the function itself is not directly invoked in any Python test.
- `__bmd_can_truncate` — never tested (always returns false; trivial stub).
- `__bmd_block_header` — never tested directly; used implicitly in write-size calculation.
- `__bmd_encrypt_skip_size` — not tested; used by the encryption layer.

**block_disagg_ckpt.c**
- `__bmd_checkpoint_pack_raw` with `root_image != NULL` — exercised by all checkpoint tests.
- `__bmd_checkpoint_pack_raw` with `root_image == NULL` (empty checkpoint) — exercised only by `test_disagg01` (the `pl_complete_checkpoint` call for checkpoint 1 before any writes).
- `__wti_block_disagg_checkpoint_load` with `addr == NULL / addr_size == 0` (empty checkpoint) — the path where the function returns 0 immediately is taken at database creation.
- `__wti_block_disagg_checkpoint_load` with a real address — exercised by every restart test.
- `__block_disagg_checkpoint_resolve` with `failed=false` — exercised implicitly by every successful checkpoint.
- `__block_disagg_checkpoint_resolve` with `failed=true` — **NOT tested** (see GAP-1).
- The `WT_DISAGG_METADATA_FILE` branch in `checkpoint_resolve` — not directly tested in isolation.
- The encryption `conn->key_provider != NULL` branch in `checkpoint_resolve` — covered by `test_key_provider_disagg01/02` (outside the target test suite).
- The `.wt_stable` suffix stripping in `checkpoint_resolve` — exercised by all layered tests.
- The else-branch (no recognized suffix) — exercised by `test_disagg01` which creates a raw `file:` URI.

**block_disagg_read.c**
- `__block_disagg_read_multiple` happy path (full-page and delta chain reads) — exercised by test_disagg01 (raw API) and by every verify/cursor-scan test.
- `__block_disagg_read_multiple` retry loop — never tested; the loop runs forever when `tmp_count == 0` (see GAP-2).
- `__block_disagg_check_lsn_frontier` with LSN ahead of frontier — covered by `test_layered34`, `test_layered39`, `test_layered66` which call `pl_set_last_materialized_lsn`, but the stat `disagg_block_read_ahead_frontier` is not asserted in any block-disagg-specific test (see GAP-3).
- Checksum mismatch / magic mismatch / compatible_version failure paths in the read loop — **NOT tested** (see GAP-4).
- The `WT_BLOCK_DISAGG_MODIFIED` (victim cache) flag path in the read loop — **NOT tested** at the integration level (see GAP-5).
- `__wti_block_disagg_read` (single-read stub) — returns ENOTSUP; not directly tested.

**block_disagg_write.c**
- `__wti_block_disagg_write_internal` happy path (base and delta) — exercised by all data-writing tests.
- `buf->size > UINT32_MAX` guard — not tested (edge case; see GAP-6).
- `data_checksum=true` vs `false` — both paths exercised; `data_checksum=true` appears in the compressed test of test_disagg_checkpoint_size01 (compressed data uses full checksums), `false` in uncompressed paths.
- `__wti_block_disagg_page_discard` with `is_root=false` — exercised by test_layered44 and implicitly by the checkpoint size leak tests.
- `__wti_block_disagg_page_discard` with `is_root=true` — taken by every checkpoint (old root page discard), but the size-accounting invariant (`bytes_total` must not double-subtract) is only verified indirectly via `test_disagg_checkpoint_size03`.
- `plh_discard == NULL` branch in `__wti_block_disagg_page_discard` — **NOT tested** (see GAP-7).

**block_disagg_open.c**
- `__wti_block_disagg_open` — exercised by all tests; the shared-handle cache hit path (ref++ branch) exercised when a table is opened twice.
- `__wti_block_disagg_close` — exercised on teardown; the `ref == 0` guard path exercised by the `bm=NULL` safety check in `__bmd_close`.
- `pl_open_handle` failure path in `__wti_block_disagg_open` — **NOT tested** (see GAP-8).

**block_disagg_addr.c**
- Pack / unpack round-trip — exercised by every read/write/checkpoint test.
- `version_min > current_version` ENOTSUP path — exercised by `test_layered68` (incompatible upgrade scenario) but only as an end-to-end test; not in the `test_disagg*` suite.
- `lsn < base_lsn_delta` EINVAL path in unpack — **NOT tested** (see GAP-9).
- `page_id == WT_BLOCK_INVALID_PAGE_ID` and `size == 0` guards in unpack — **NOT tested** (see GAP-9).
- Cookie size mismatch check at end of unpack — **NOT tested** (see GAP-9).
- Debug upgrade / optional field paths — covered by `test_layered68`.

**block_disagg_unsup.c**
- All stubs return 0 or false; no meaningful branches. Not worth dedicated tests.

---

## Duplicate / Overlapping Cases

### [DUP-1] Checkpoint size monotonicity overlaps across test_disagg_checkpoint_size01 and test_disagg_checkpoint_size02

- **Tests involved:** `test_disagg_checkpoint_size01.test_checkpoint_size_increases` and `test_disagg_checkpoint_size02.test_database_size_increases`
- **Overlap:** Both tests verify that inserting data and taking a second checkpoint produces a strictly larger size than the first checkpoint. The difference is only the location where the size is read (stable-file metadata vs. checkpoint completion record). Both use the same workload pattern (insert N rows, checkpoint, insert more rows, checkpoint, assert growth).
- **Recommendation:** Keep both: they validate two independent size-tracking fields (`ckpt->size` in btree metadata vs. `database_size` in page log completion). Add a comment in each test clarifying which size field is being validated so the distinction is obvious to readers.

### [DUP-2] Post-restart size persistence checked in both checkpoint size test files

- **Tests involved:** `test_disagg_checkpoint_size01.test_checkpoint_size_persists_across_restart` and `test_disagg_checkpoint_size02.test_database_size_persists_across_restart`
- **Overlap:** Both reopen the connection after writing+checkpointing, then assert size equality (or near-equality). Both use `expectedStdoutPattern("Removing local file")` as the guard.
- **Recommendation:** Keep both because they test different serialization paths. The tolerance in test02 (10% delta) is loose due to shutdown checkpoints; consider tightening with a `disagg_advance_checkpoint` call before restart if the palite backend supports it.

### [DUP-3] Cold-read stat and cold-write stat tested separately but with identical setup

- **Tests involved:** `test_disagg04.test_cold_write` and `test_disagg04.test_cold_read`
- **Overlap:** Both create a `storage_tier=cold` layered table and call `add_data(uri, 1000)`. The write test stops there; the read test adds a `verifyUntilSuccess` call. The setup code is essentially duplicated.
- **Recommendation:** Minor cosmetic issue; combining them into one test risks obscuring which stat was zero before the write (the cold-read test correctly shows the read stat is zero after the write). Keep separate but consider extracting the common setup into a helper method.

---

## Missing Coverage

### [CRITICAL] GAP-1: `checkpoint_resolve` with `failed=true` is never tested

**What is not tested:**
`__block_disagg_checkpoint_resolve` has an early-return branch `if (failed) return (0)` at line 158 of `block_disagg_ckpt.c`. This branch is invoked by `meta_track.c:186` when checkpoint fails. If a bug caused this branch to be removed or skipped, checkpoint failure would cause the metadata enqueue operation to run on a partially-written checkpoint, potentially persisting a corrupt metadata record to the shared metadata table.

**Risk:** A bug in the `failed=true` path means a failed checkpoint could incorrectly commit metadata updates to the shared metadata table (e.g., via `__wt_disagg_enqueue_metadata_operation`), causing followers to see inconsistent or corrupt shared metadata. In a disaggregated setting where metadata is the source of truth for all readers, this could silently corrupt the visible state of every table in the database for all nodes.

**Code path analysis:**
- In `src/block_disagg/block_disagg_ckpt.c` function `__block_disagg_checkpoint_resolve()`: the `if (failed) return (0)` at line 158 must short-circuit before any metadata enqueue operation.
- This path is triggered when: `bm->checkpoint_resolve(bm, session, true)` is called by `src/meta/meta_track.c:186` (the rollback path in `__wt_meta_track_checkpoint`), which fires when the checkpoint transaction fails after block-manager writes have started.
- Why existing tests don't reach it: all successful-checkpoint tests (test_disagg_checkpoint_size01-04, test_disagg01-04) complete without error. `test_disagg_checkpoint_size02.test_failed_checkpoint_no_size_change` simulates a crash but uses `simulate_crash_restart`, which copies the home directory and reopens — it does not inject a failure into the in-progress `__wti_block_disagg_checkpoint_resolve` call itself. No test drives the system into the state where `checkpoint_resolve` is called with `failed=true`.

**Proposed test design:**
- Setup: Open a disaggregated connection in leader mode with a layered table. Insert sufficient data that at least one reconciliation pass occurs.
- Operations: Use a fault-injection hook (or a WiredTiger debug-mode connection config if available) to simulate a failure during checkpoint commit, triggering the `failed=true` path. Alternatively, use a mock or shim that records whether `__wt_disagg_enqueue_metadata_operation` was called. The key assertion is that after a simulated checkpoint failure, the shared metadata is NOT updated (the enqueue is not called).
- Assertions: After simulated failure, reopen the connection and verify that the stable-file metadata matches the pre-failure state (not the post-failure tentative state). Verify the `disagg_abandon_checkpoint_succeed` stat (set in `conn_layered.c`) incremented.
- Why this is sufficient: It directly exercises the branch, and the metadata consistency check is the exact invariant the branch protects.
- Proposed location: `test/suite/test_disagg05.py` or a new `test_disagg_checkpoint_failure.py`.

---

### [CRITICAL] GAP-2: The infinite retry loop in `__block_disagg_read_multiple` is never tested

**What is not tested:**
In `block_disagg_read.c` lines 157–176, when `plh_get` returns 0 results (`tmp_count == 0`), the code retries indefinitely with a growing sleep (`__wt_sleep(0, WT_MIN(10000 + retry * 5000, 500000))`). The comment explicitly acknowledges this is a permanent hang by design ("we never give up") — FIXME WT-15768. No test verifies that: (a) the retry loop actually triggers, (b) the verbose notice message is emitted, or (c) the system can make progress again after the page service recovers.

**Risk:** If the retry logic has a bug (e.g., the `tmp_count` variable is not reset between retries, or `results_array` is not properly re-initialized), a transient page-service hiccup could cause a permanent hang or a silent stale-read from a previous iteration's buffer. In production, this manifests as a frozen session with no diagnostic output after the initial retry notice, making root-cause analysis very hard.

**Code path analysis:**
- In `src/block_disagg/block_disagg_read.c` function `__block_disagg_read_multiple()`: the loop `for (retry = 0, tmp_count = 0; tmp_count == 0; retry++)` executes the `plh_get` call and then checks `tmp_count`. If the page service returns no results (empty response without an error), the loop retries.
- This path is triggered when: `plh_get` succeeds (returns 0) but sets `*results_count = 0` — a condition currently possible in `palite` if the page is not yet visible at the requested LSN.
- Why existing tests don't reach it: all existing tests write data and then immediately read it, so `palite` always has the requested page available. No test withholds a page write before attempting a read.

**Proposed test design:**
- Setup: Use the `palite` page log at the Python API level (as in `test_disagg01`). Open a handle for a table. Put a page write and record its LSN. Before completing the write's checkpoint, attempt a read at an LSN that doesn't have the page yet, so palite returns 0 results.
- Operations: Call `plh_get` on a page ID for which no put has been issued at the requested LSN. Observe that at least one retry occurs (check the `WT_VERB_READ` log or a stat).
- Assertions: The retry log message `"retry #N for page_id..."` appears in verbose output. After the page is written (in a separate thread or via a timeout mechanism), the read eventually succeeds.
- Caveat: Because the loop is infinite (WT-15768 is unresolved), a test for the retry loop must use a time-bounded approach (e.g., a background thread that completes the page write after a delay). The simpler near-term test is to verify the retry notice message is emitted on a single retry by writing the page immediately after the first failed get in a controlled sequence.
- Proposed location: `test/suite/test_disagg05.py` or a new catch2 unit test in `test/catch2/disagg/`.

---

### [CRITICAL] GAP-3: LSN frontier check stat (`disagg_block_read_ahead_frontier`) is never asserted in block-disagg tests

**What is not tested:**
`__block_disagg_check_lsn_frontier` in `block_disagg_read.c` (lines 81–97) checks that the page LSN is not ahead of the materialization frontier. When the check fires it increments `disagg_block_read_ahead_frontier` and logs a verbose warning. The FIXME-WT-15818 comment notes that crashing on this violation may be appropriate. No test in `test_disagg*` or `test_disagg_checkpoint_size*` verifies that (a) the frontier check fires, (b) the stat is incremented, or (c) a read of a page with LSN exactly at or below the frontier does NOT fire the warning.

**Risk:** If the frontier check is silently broken (e.g., the `last_materialized_lsn` value is not loaded, or the comparison is inverted), reads that should be blocked could silently return stale or inconsistent data to followers. Since the comment explicitly considers escalating to a crash, any gap between the check's intended and actual behavior in production could mean silent data corruption rather than a detectable failure.

**Code path analysis:**
- In `src/block_disagg/block_disagg_read.c` function `__block_disagg_check_lsn_frontier()`: the condition `lsn > last_materialized_lsn` triggers when reading a page whose LSN is ahead of the frontier. The special values `WT_DISAGG_LSN_NONE` and `WT_DISAGG_START_LSN` skip the check.
- `test_layered34`, `test_layered39`, `test_layered66` exercise the materialization frontier via `pl_set_last_materialized_lsn`, but they do not directly assert the `disagg_block_read_ahead_frontier` connection stat.
- Why existing disagg-specific tests don't reach it: all `test_disagg*` tests operate as leader with no follower reading behind the frontier; no test configures a `last_materialized_lsn` and then performs a read at an LSN ahead of it.

**Proposed test design:**
- Setup: Open a disaggregated connection as follower. Write pages (via leader) up to LSN N. Set `last_materialized_lsn` to N-1 via `conn.reconfigure` or `pl_set_last_materialized_lsn`.
- Operations: Open a cursor and read a page that was written at LSN N (ahead of frontier N-1).
- Assertions: `stat.conn.disagg_block_read_ahead_frontier` is greater than 0 after the read. Also assert the verbose warning message appears. Separately assert that reading at LSN <= N-1 does NOT increment the stat.
- Proposed location: `test/suite/test_disagg05.py` or extend `test_layered39` to add the stat assertion.

---

### [CRITICAL] GAP-4: Checksum mismatch, magic mismatch, and `compatible_version` failure in the read loop are never tested

**What is not tested:**
In `__block_disagg_read_multiple` (lines 216–303 of `block_disagg_read.c`), three corruption detection branches exist:
1. `swap.checksum != checksum` (header checksum mismatch, line 285): logs an error and jumps to `corrupt`.
2. `swap.magic != expected_magic` (magic mismatch, line 225): logs an error and jumps to `corrupt`.
3. `swap.compatible_version > WT_BLOCK_DISAGG_COMPATIBLE_VERSION` (version too new, line 232): logs an error and jumps to `corrupt`.
4. The `corrupt:` label (line 291) itself: sets `WT_CONN_DATA_CORRUPTION`, dumps the block, and calls `WT_ERR_PANIC`.

No test ever exercises any of these paths. The `WT_SESSION_QUIET_CORRUPT_FILE` suppression path (which returns `WT_ERROR` instead of panicking) is also untested.

**Risk:** If any of these checks is accidentally disabled or inverted (e.g., by a merge conflict in the flags logic), corrupted pages from the page service would be silently accepted as valid data. Because the disaggregated layer has no local file to fall back to, once a corrupted page is returned by `plh_get`, the only protection is this code. A bug here means silent silent data corruption with no diagnostic output.

**Code path analysis:**
- The `if (F_ISSET(&swap, WT_BLOCK_DISAGG_MODIFIED) || swap.checksum == checksum)` outer condition (line 218) gates the full checksum comparison. A page where the stored checksum matches the expected value is fully checked; a MODIFIED page (from victim cache) is not (see GAP-5). Within the inner `if (__wt_checksum_match(...))` block, the magic and version checks run.
- This path is triggered when: `plh_get` returns a buffer whose `WT_BLOCK_DISAGG_HEADER` contains a wrong `checksum`, `magic`, or `compatible_version` field.
- Why existing tests don't reach it: `palite` always returns the exact bytes that were put in, so the header is never corrupt in normal operation. No test injects a corrupted buffer.

**Proposed test design:**
- Setup: Use a palite mock or a shim at the `plh_get` layer that intercepts the returned buffer and flips bytes in the block header before returning.
- Operations (three separate test cases):
  1. Flip the `checksum` field → assert `WT_CONN_DATA_CORRUPTION` is set; because panic occurs in non-quiet sessions, use `WT_SESSION_QUIET_CORRUPT_FILE` or a subprocess approach (similar to `test_key_provider_disagg02`) and assert `WT_ERROR` is returned.
  2. Set `magic` to an invalid value (not `0xdb` or `0xdd`) → same assertions.
  3. Set `compatible_version` to a value > `WT_BLOCK_DISAGG_COMPATIBLE_VERSION` → same assertions.
- Assertions: `WT_CONN_DATA_CORRUPTION` flag is set on the connection; the error code is `WT_ERROR` (in quiet mode) or causes a panic (in normal mode, captured via subprocess); the block dump log message `"corrupt dump:"` appears.
- Proposed location: `test/suite/test_disagg_corrupt.py` (new file) or a catch2 unit test that mocks `plh_get`.

---

### [HIGH] GAP-5: The `WT_BLOCK_DISAGG_MODIFIED` (victim cache) path in the read loop is never tested

**What is not tested:**
In `__block_disagg_read_multiple` (line 216), when `F_ISSET(&swap, WT_BLOCK_DISAGG_MODIFIED)` is true, the code sets `from_cache = true` and skips the checksum match against the cookie's expected checksum (TODO WT-16511). At the end of the function (line 312), when `from_cache` is true, the cumulative-size assertion is also skipped because victim-cache pages are a different compressed format. No test drives any block through the victim cache path — `test_layered43`, the only disagg test using `block_cache=(enabled=true)`, is permanently skipped (`early_setup` calls `self.skipTest("FIXME-WT-15663")`).

**Risk:** The victim cache code path includes two deliberate safety bypasses (checksum skip and size-assert skip). If a future change incorrectly sets `WT_BLOCK_DISAGG_MODIFIED` on non-cache pages, checksums would be silently skipped for those pages. Conversely, if the path is broken, victim-cache reads silently degrade to uncached reads with incorrect metadata.

**Code path analysis:**
- In `src/block_disagg/block_disagg_read.c` lines 216–217: `F_ISSET(&swap, WT_BLOCK_DISAGG_MODIFIED)` causes `from_cache = true` and enters the checksum comparison block with a different contract.
- Lines 246–249: `block_meta->delta_count` is set differently for MODIFIED vs. non-MODIFIED pages.
- Lines 312–317: cumulative size assertion is skipped when `from_cache`.
- Why existing tests don't reach it: `test_layered43` is the only disagg test enabling the block cache, and it self-skips. No other test configures `block_cache=(enabled=true)` together with disaggregated storage.

**Proposed test design:**
- Setup: Re-enable the victim cache for disaggregated storage once FIXME-WT-15663 is resolved. Until then, add a lower-level unit test using a mock `plh_get` that returns a buffer with `WT_BLOCK_DISAGG_MODIFIED` set.
- Operations: Write a page, then read it back via a mock `plh_get` that sets the MODIFIED flag and returns a differently-sized buffer.
- Assertions: The read succeeds; `from_cache` is implicitly true (verified by the absence of the cumulative-size assertion in the test path); `block_meta->delta_count` matches the value from `get_args.delta_count`, not `(*results_count - 1)`.
- Proposed location: `test/catch2/disagg/test_block_disagg_read.cpp` (new catch2 file with a mock page log handle).

---

### [HIGH] GAP-6: `plh_discard == NULL` branch in `__wti_block_disagg_page_discard` is never tested

**What is not tested:**
In `block_disagg_write.c` (lines 315–318), if `plhandle->plh_discard == NULL`, the function logs a warning and returns 0, silently skipping the actual discard. No test configures a page log handle where `plh_discard` is NULL and verifies this code is safe to call.

**Risk:** A page log implementation that legitimately omits `plh_discard` (treating discards as no-ops) will silently accumulate unreachable pages in the remote store. If the warning path is broken (e.g., the NULL check is removed in a refactor), calling a NULL function pointer causes an immediate crash. The combined effect — silent accumulation in valid implementations, crash in refactored code — is an important correctness and stability gap.

**Code path analysis:**
- In `src/block_disagg/block_disagg_write.c` function `__wti_block_disagg_page_discard()`: after unpacking the cookie, it checks `plhandle->plh_discard == NULL` and returns 0 with a warning.
- This path is triggered when: the page log implementation does not implement `plh_discard`.
- Similarly, `__bmd_get_page_ids` in `block_disagg_mgr.c` (line 154) has the analogous `plh_get_page_ids == NULL` warning-and-return path. Both are untested.

**Proposed test design:**
- Setup: Create a minimal mock `WT_PAGE_LOG_HANDLE` where `plh_discard` is set to NULL. Use it in a unit test (catch2) that calls `__wti_block_disagg_page_discard` directly.
- Operations: Call the discard function with a valid address cookie and `is_root=false`.
- Assertions: The function returns 0 (no error). The warning message `"plh_discard is not implemented"` appears in verbose output. `disagg_block_page_discard` stat is NOT incremented (the stat increment at line 331 occurs before the NULL check, so this may already be an existing bug to catch).
- Proposed location: `test/catch2/disagg/test_block_disagg_discard.cpp` (new) or extend `test_page_log_handle.cpp`.

---

### [HIGH] GAP-7: `is_root=true` in `__bmd_free` / `__wti_block_disagg_page_discard` has no explicit size-accounting invariant test

**What is not tested:**
`__bmd_free` in `block_disagg_mgr.c` accepts an `is_root` boolean and passes it to `__wti_block_disagg_page_discard`. When `is_root=true`, `__wt_btree_decrease_size` is deliberately skipped. The comment in `block_disagg_write.c` (lines 298–310) explains the invariant: the old root page size is included in the checkpoint's recorded size, and `__bmd_checkpoint_pack_raw` already subtracts `previous_root_size` — so a second subtraction here would undercount. No test explicitly verifies this invariant holds: that `bytes_total` equals the expected value after a checkpoint that replaces the root page.

**Risk:** If a future refactor of `__bmd_checkpoint_pack_raw` changes how `previous_root_size` is handled, or if `is_root` is accidentally passed as `false` in a call site for a root-page free, the size will be double-subtracted. This produces a `bytes_total` that is too small, causing the database size to shrink incorrectly with each checkpoint. The `test_disagg_checkpoint_size03` tests bound the *relative* size but do not isolate the root-page-specific accounting.

**Code path analysis:**
- In `src/block_disagg/block_disagg_write.c` function `__wti_block_disagg_page_discard()`: line 311 `if (!is_root) __wt_btree_decrease_size(session, cookie.size)`.
- `is_root=true` is passed by `bm->free(bm, session, addr, addr_size, true)` which is called when the old root page address is freed after the new root page is written.
- Why existing tests don't reach it explicitly: the size-accounting tests (`test_disagg_checkpoint_size01-04`) check the aggregate database size but not the root-page-specific invariant. None of them read `bytes_total` before and after a root-page replacement and verify the difference equals exactly the net change (new root size minus old root size).

**Proposed test design:**
- Setup: Use a single-table disaggregated database. Write a small initial dataset and checkpoint to establish a baseline `bytes_total` from the completion record.
- Operations: Repeatedly rewrite all data and checkpoint, forcing a new root page each time (enough data to fill at least a root page). After each cycle, read `database_size` from the completion record.
- Assertions: Across N rewrite cycles where data volume is constant, `database_size` should converge to a stable value rather than growing monotonically. Specifically, the difference between cycle N and cycle 1 should be less than 10% of the baseline. This directly catches double-subtraction of the root page size.
- Proposed location: `test/suite/test_disagg_checkpoint_size05.py` (new file, extending the checkpoint-size series).

---

### [HIGH] GAP-8: Address cookie unpack error paths are never tested at the block-layer level

**What is not tested:**
`__wt_block_disagg_addr_unpack` in `block_disagg_addr.c` has four explicit error returns:
1. `version_min > current_version` → ENOTSUP (line 169).
2. `lsn < base_lsn_delta` → EINVAL (line 202) — LSN underflow.
3. `page_id == WT_BLOCK_INVALID_PAGE_ID` → EINVAL (line 209).
4. `size == 0` → EINVAL (line 212).
5. Cookie byte-count mismatch → EINVAL (line 233).

These errors are tested at the address-cookie level by `test_layered68` (version mismatch) but not at the level of a function that calls `__wt_block_disagg_addr_unpack` and must handle its failure: `__wti_block_disagg_read_multiple`, `__wti_block_disagg_page_discard`, `__wti_block_disagg_addr_string`, and `__wti_block_disagg_addr_invalid`.

**Risk:** If `__wti_block_disagg_read_multiple` does not propagate the error from `__wt_block_disagg_addr_unpack` (which it does via `WT_RET`), a corrupt address cookie in the btree's internal page would cause a read to silently proceed with garbage `page_id`, `lsn`, and `checksum` values, requesting the wrong page from the page service. The LSN-underflow case (lsn < base_lsn_delta) is particularly dangerous because base_lsn would wrap to a very large value, causing the page service to return unexpected data.

**Code path analysis:**
- In `src/block_disagg/block_disagg_read.c` function `__wti_block_disagg_read_multiple()`: line 360 calls `__wt_block_disagg_addr_unpack`, and its error is returned via `WT_RET`. The call correctly propagates errors, but propagation is not tested.
- In `src/block_disagg/block_disagg_write.c` function `__wti_block_disagg_page_discard()`: line 287 has the same pattern.
- Why existing tests don't reach it: all address cookies in production tests are produced by `__wti_block_disagg_addr_pack`, which generates valid cookies. No test crafts a malformed address cookie and feeds it to the read or discard path.

**Proposed test design:**
- Setup: Use the catch2 framework to construct deliberately malformed address cookie byte strings.
- Operations (separate sections):
  1. Craft a cookie with `lsn=5, base_lsn_delta=10` (underflow): call `__wt_block_disagg_addr_unpack` directly and assert EINVAL.
  2. Craft a cookie with `page_id = WT_BLOCK_INVALID_PAGE_ID`: assert EINVAL.
  3. Craft a cookie with `size=0`: assert EINVAL.
  4. Craft a valid cookie but truncate by 1 byte: assert EINVAL (size mismatch).
- Assertions: Each malformed cookie returns the expected error code; no out-of-bounds reads occur (run under AddressSanitizer).
- Proposed location: Extend `test/catch2/disagg/test_block_disagg_addr.cpp` (new file).

---

### [HIGH] GAP-9: `pl_open_handle` failure path in `__wti_block_disagg_open` is never tested

**What is not tested:**
In `block_disagg_open.c` (line 119), if `S2BT(session)->page_log->pl_open_handle(...)` fails, the `err:` label calls `__block_disagg_destroy`. The destroy function removes the block from the connection hash, frees the name, calls `plh_close` (which may be NULL if `pl_open_handle` failed before setting `plhandle`), and frees the structure. If `plh_close` is called on a non-NULL but partially initialized `plhandle`, behavior is undefined.

**Risk:** A failing `pl_open_handle` in production (e.g., page service is unreachable at table-open time) may cause a use-after-free or null-pointer dereference in the error path. Since `block_disagg_destroy` calls `plh_close` on whatever `plhandle` was set before the error, any partial initialization of the handle could be exploited.

**Code path analysis:**
- In `src/block_disagg/block_disagg_open.c` function `__wti_block_disagg_open()`: if `pl_open_handle` returns non-zero, `err:` label fires, calling `__block_disagg_destroy(session, block_disagg)`. At this point `block_disagg->plhandle` may be NULL (if `pl_open_handle` set it to NULL on failure) or may be a partially-initialized handle.
- In `__block_disagg_destroy` (line 53): `if (block_disagg->plhandle != NULL) WT_TRET(block_disagg->plhandle->plh_close(...))`. If `plhandle` is non-NULL but `plh_close` is NULL, this dereferences a null function pointer.
- Why existing tests don't reach it: `palite` always succeeds at `pl_open_handle`. No test uses a mock that returns an error from `pl_open_handle`.

**Proposed test design:**
- Setup: Construct a mock `WT_PAGE_LOG` implementation where `pl_open_handle` returns EINVAL.
- Operations: Call `__wti_block_disagg_open` with this mock page log.
- Assertions: `__wti_block_disagg_open` returns EINVAL. No crash or sanitizer violation. The block is removed from the connection hash (verify `conn->blockhash` does not contain the failed entry). `block_disagg` is freed (run under AddressSanitizer to verify no leak).
- Proposed location: `test/catch2/disagg/test_block_disagg_open.cpp` (new) or extend `test_page_log_handle.cpp`.

---

### [MEDIUM] GAP-10: Block handle reference-count sharing (shared handle cache) is not verified

**What is not tested:**
In `__wti_block_disagg_open` (lines 91–99), if a block for the same filename is already in the connection hash, the reference count is incremented and the existing handle is returned. Conversely, `__wti_block_disagg_close` decrements the ref count and only destroys the handle when `ref` reaches 0. No test opens the same disaggregated block handle from two concurrent sessions and verifies that the second open returns the same handle (without calling `pl_open_handle` again), and that the handle is only closed when the last reference is released.

**Risk:** If the ref-count logic has an off-by-one error or a race condition (e.g., the ref count is decremented twice on close due to the `block_disagg->ref == 0 || --block_disagg->ref == 0` guard), a session could use a freed block handle, causing a use-after-free. Conversely, if the ref count is never decremented (leak), the page log handle is never closed, potentially leaking resources in the page service.

**Code path analysis:**
- In `src/block_disagg/block_disagg_open.c` function `__wti_block_disagg_open()`: lines 91–98 check the hash bucket for an existing block; if found, `++block->ref` and return early without calling `pl_open_handle`.
- In `__wti_block_disagg_close()`: line 156 `if (block_disagg->ref == 0 || --block_disagg->ref == 0)` — the first condition `ref == 0` guards against a double-close, but also means a block with ref=0 would be re-destroyed. This could be a latent bug if `ref` somehow reaches 0 before close is called.

**Proposed test design:**
- Setup: Open two sessions on the same disaggregated connection. Open the same layered table URI from both sessions simultaneously.
- Operations: Write data from session 1. Read from session 2. Close session 1 (should not destroy the handle). Verify reads from session 2 still work. Close session 2 (handle should now be destroyed).
- Assertions: `pl_open_handle` is called exactly once (verify via a stat or a counter in the mock). After session 1 closes, the block's ref count is 1. After session 2 closes, the block is removed from the hash and `plh_close` is called exactly once.
- Proposed location: `test/suite/test_disagg05.py` or `test/catch2/disagg/test_block_disagg_open.cpp`.

---

### [MEDIUM] GAP-11: Empty-checkpoint path (`root_image == NULL`) is not tested at the integration level

**What is not tested:**
In `__bmd_checkpoint_pack_raw` (lines 37–39), when `root_image == NULL`, the checkpoint cookie is set to NULL/0 size. This path is taken for tables that were not modified since the last checkpoint ("fake checkpoint"). While `test_disagg01` calls `pl_complete_checkpoint` for an empty checkpoint at the raw Python API level, no integration test verifies that:
1. Taking a checkpoint on an unmodified layered table produces a NULL address cookie.
2. `__wti_block_disagg_checkpoint_load` correctly handles a zero-size address (the `if (addr == NULL || addr_size == 0) return (0)` path at line 253–254).
3. After restart, a table whose last checkpoint had a NULL cookie is correctly identified as empty (no root page is loaded).

**Risk:** If the `root_image == NULL` handling in pack or unpack has a bug (e.g., the NULL check in `checkpoint_load` is removed), an empty checkpoint would attempt to unpack a zero-length cookie, hitting an unpack error or reading garbage data. This would prevent reopening any table that had no writes since the last checkpoint, effectively making read-only replica behavior broken.

**Code path analysis:**
- In `src/block_disagg/block_disagg_ckpt.c` function `__bmd_checkpoint_pack_raw()`: `if (root_image == NULL)` sets `ckpt->raw.data = NULL; ckpt->raw.size = 0`.
- In `__wti_block_disagg_checkpoint_load()`: `if (addr == NULL || addr_size == 0) return (0)` — this guard is what makes empty checkpoints safe to load.
- Why existing tests don't reach it: all checkpoint tests write data before checkpointing. The `test_disagg01` raw API test calls `pl_complete_checkpoint` at the page log level, but does not go through `__bmd_checkpoint_pack_raw`.

**Proposed test design:**
- Setup: Create a layered table. Insert data and checkpoint (non-empty). Do NOT insert any more data. Take a second checkpoint immediately.
- Operations: After the second (empty) checkpoint, reopen the connection. Verify all previously written data is still readable.
- Assertions: The second checkpoint's cookie for the btree is zero-length (verify via the metadata cursor that `size=0` or the checkpoint entry has no raw data). After restart, a cursor scan returns all rows from the first checkpoint. No panic or error during load.
- Proposed location: `test/suite/test_disagg05.py`.

---

### [MEDIUM] GAP-12: `__wti_block_disagg_write_size` EINVAL path (buffer too large) is never tested for disagg

**What is not tested:**
`__wti_block_disagg_write_size` returns EINVAL when `*sizep > UINT32_MAX - 1024`. This is a boundary guard against writing a single page larger than ~4 GB. The catch2 block API tests (`test_block_api_write.cpp`) test this guard for the standard block manager, but not for the disaggregated block manager.

**Risk:** Low — WiredTiger already limits key/value sizes well below 4 GB. However, if a future change to the write-size calculation introduces an overflow, the guard's absence in disagg-specific tests means the guard could be silently disabled for disagg tables.

**Code path analysis:**
- In `src/block_disagg/block_disagg_write.c` function `__wti_block_disagg_write_size()`: `return (*sizep > UINT32_MAX - 1024 ? EINVAL : 0)`.
- The standard block manager's equivalent is tested in `test_block_api_write.cpp:166–168`.

**Proposed test design:**
- Add a catch2 section to a new `test/catch2/disagg/test_block_disagg_write.cpp` that calls `__wti_block_disagg_write_size` with `*sizep = UINT32_MAX` and asserts EINVAL, and with `*sizep = 0` and asserts 0.

---

### [LOW] GAP-13: `__bmd_stat` / `__wti_block_disagg_stat` only writes `block_magic`; no disagg-specific stats are verified

**What is not tested:**
`__wti_block_disagg_stat` in `block_disagg_open.c` writes only `block_magic` to the data-source stats structure. The comment says "Fill this out." No test verifies that the stat cursor for a disaggregated table returns the correct `block_magic` value, or that future disagg-specific stats are correctly plumbed through this function.

**Risk:** Low — the function is a stub. Risk increases as disagg-specific stats are added here without a test to catch wiring errors.

**Proposed test design:**
- Open a stat cursor on a disaggregated table URI (`statistics:layered:...`). Assert that `stat.dsrc.block_magic` equals `WT_BLOCK_MAGIC` (16352). This is a trivial one-liner to add to any existing disagg test.

---

### [LOW] GAP-14: `checkpoint_resolve` non-.wt / non-.wt_stable suffix (else branch) not covered by a named test

**What is not tested:**
In `__block_disagg_checkpoint_resolve` (lines 199–201), if the filename has neither `.wt` nor `.wt_stable` suffix, the table name is used as-is. This branch exists for test files created without a suffix (comment: "This can happen if the 'file:' is created without a suffix in our tests"). No current test in the `test_disagg*` series explicitly exercises this branch with a checkpoint commit.

`test_disagg01` uses a raw `file:` URI indirectly but calls `page_log.pl_complete_checkpoint` at the Python API level rather than going through `checkpoint_resolve`. This branch may be dead code in production (all real tables have `.wt` or `.wt_stable` suffixes), but if the suffix detection logic changes, the else branch could incorrectly handle production filenames.

**Proposed test design:**
- Add a sub-case to an existing layered test (or a new `test_disagg05.py`) that creates a `file:no_suffix` table and performs a full checkpoint cycle. Assert that the checkpoint resolves without error and that the metadata is correctly enqueued.

---

## Summary Table

| Priority | Gap | Risk | Proposed Test Location |
|---|---|---|---|
| CRITICAL | GAP-1: `checkpoint_resolve(failed=true)` never tested | Corrupt shared metadata after failed checkpoint | `test/suite/test_disagg05.py` |
| CRITICAL | GAP-2: Infinite retry loop in `__block_disagg_read_multiple` never tested | Undetectable hang or stale buffer reuse | `test/suite/test_disagg05.py` or `test/catch2/disagg/` |
| CRITICAL | GAP-3: `disagg_block_read_ahead_frontier` stat never asserted | Silent reads past materialization frontier go undetected | `test/suite/test_disagg05.py` or extend `test_layered39` |
| CRITICAL | GAP-4: Checksum/magic/version corruption detection in read loop never tested | Corrupt pages silently accepted; no detection of broken guards | `test/suite/test_disagg_corrupt.py` (new) |
| HIGH | GAP-5: `WT_BLOCK_DISAGG_MODIFIED` (victim cache) read path never tested | Checksum bypass silently applied to non-cache pages if flag misset | `test/catch2/disagg/test_block_disagg_read.cpp` (new) |
| HIGH | GAP-6: `plh_discard == NULL` and `plh_get_page_ids == NULL` branches never tested | Crash if NULL check removed in refactor; silent stat miscount | `test/catch2/disagg/test_block_disagg_discard.cpp` (new) |
| HIGH | GAP-7: `is_root=true` size-accounting invariant not explicitly tested | Double-subtraction of root page size corrupts `bytes_total` across checkpoints | `test/suite/test_disagg_checkpoint_size05.py` (new) |
| HIGH | GAP-8: Address cookie unpack error paths not tested at block-layer callers | Malformed cookie causes wrong page request; no error propagation verification | `test/catch2/disagg/test_block_disagg_addr.cpp` (new) |
| HIGH | GAP-9: `pl_open_handle` failure in `__wti_block_disagg_open` not tested | Use-after-free / null-deref in error cleanup path | `test/catch2/disagg/test_block_disagg_open.cpp` (new) |
| MEDIUM | GAP-10: Block handle ref-count sharing not verified | Use-after-free on premature handle destruction; resource leak | `test/suite/test_disagg05.py` |
| MEDIUM | GAP-11: Empty checkpoint (`root_image == NULL`) not tested at integration level | Empty checkpoint load fails, breaking read-only replicas | `test/suite/test_disagg05.py` |
| MEDIUM | GAP-12: `write_size` EINVAL path not tested for disagg block manager | Write-size overflow guard silently broken for disagg tables | `test/catch2/disagg/test_block_disagg_write.cpp` (new) |
| LOW | GAP-13: `__bmd_stat` only writes `block_magic`; not verified | Future disagg stats added without test wiring | Any existing disagg Python test |
| LOW | GAP-14: Non-.wt/.wt_stable suffix else-branch in `checkpoint_resolve` not named | Dead code or production suffix change silently undetected | `test/suite/test_disagg05.py` |
