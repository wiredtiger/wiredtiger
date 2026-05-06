# Gap Analysis: C-Level Tests (csuite), Unit Tests (catch2), and Stress Tests (cppsuite)

---

## Current Coverage Structure

### csuite (41 tests)
The csuite covers crash/recovery (random_abort, schema_abort, timestamp_abort), checkpoint integrity
(wt2909, wt3363, wt6616, wt9199), compaction (wt7989, wt8057, wt8246, wt10897), backup (incr_backup,
wt12015), metadata salvage (wt4156), concurrency primitives (rwlock, wt2535, wt8963, wt10461),
cursor/API correctness (scope, wt3338, wt3184, wt3874, wt4105, wt6185), configuration parsing
(wt11126, wt11440), and various component smoke tests (wt3120, wt2695, wt4117, normalized_pos,
random_session, wt4333, wt4891, wt13867). **Zero csuite tests cover disaggregated storage or layered
tables.**

### catch2 (60+ tests)
Tests range from block manager internals (12 block unit tests covering addr, checksum, extent lists,
sessions), live restore file system (10 tests), sub-level error API (12 tests), cursor/API (3 tests),
to disagg-specific tests. The disagg catch2 coverage is: `test_layered_incomplete_table` (8 metadata
combinations for open/reopen), `test_page_log_handle` (3 handle lifecycle cases),
`test_disagg_meta_config` (~25 parse cases), `ext_test_checkpoint_meta_version` (7 version validation
cases), `ext_test_key_provider` (8 key state machine tests), `ext_test_key_provider_header` (5
header ABI tests). The block/ catch2 tests are entirely for the regular block manager (`block.c`,
`block_ckpt.c`, `block_ext.c`); there are no parallel tests for `block_disagg/`.

### cppsuite (15 tests)
General-purpose stress tests covering bounded cursor correctness and performance, history store
cleanup, background compaction, burst inserts, cache resize, reverse splits, live restore, and API
benchmarks. **One disagg-specific test exists: `test_disagg_failover_perf`**, which measures
leader-to-follower-to-leader step-up latency without asserting a bound, and requires the proprietary
palite extension. No disagg correctness stress test exists.

### Python suite (95 layered tests, test_layered01–test_layered97)
Largely functional and API-level tests exercising the layered table interface. They test history store
interaction (test_layered25), backup, cursor operations, and role transitions but are not crash tests
and do not test deep internals.

---

## Duplicate / Overlapping Cases

### [DUP-1] csuite: Three compaction interrupt tests with significant scenario overlap

- **Tests involved:** `wt8057_compact_stress`, `wt10897_compact_quick_interrupt`, `wt8246_compact_rts_data_correctness`
- **Overlap:** All three tests interrupt compaction via crash or event handler. `wt8057` and `wt10897`
  both test compaction being stopped early (one via SIGKILL, one via `WT_EVENT_COMPACT_CHECK`), and
  both verify no data corruption results. The compaction+checkpoint concurrent interaction in
  `wt7989_compact_checkpoint` partially overlaps with `wt8057` in testing concurrent compaction
  operations under timing stress, though `wt7989` focuses on deadlock/progress while `wt8057` focuses
  on crash consistency.
- **What is distinct:** `wt8246` uniquely covers RTS interaction after a crash mid-compact, which is
  non-overlapping. `wt10897` uniquely tests the pre-review interrupt (pages_reviewed == 0) case.
  `wt8057` uniquely tests the two-table consistency invariant across the crash boundary.
- **Recommendation:** The tests are sufficiently distinct in their core assertion to justify
  coexistence. However, `wt8057` and `wt7989` share the pattern of concurrent compact+checkpoint
  under timing stress; consider whether `wt8057`'s checkpoint thread logic adds marginal value beyond
  `wt7989`. No immediate consolidation needed, but new compaction tests should evaluate whether they
  add truly new failure modes.

### [DUP-2] csuite: Two CRC32C checksum tests

- **Tests involved:** `wt2695_checksum`, `wt4117_checksum`
- **Overlap:** Both test the CRC32C implementation against the same six known reference values
  (null×1–4, "123456789", "The quick brown fox..."). `wt2695` goes further (random data,
  cumulative/seeded checksums, misalignment strobing), but the known-value subset is 100% duplicated
  in `wt4117`.
- **Recommendation:** `wt4117` is a pure subset of `wt2695`. `wt4117` could be removed or its
  known-value cases merged into `wt2695`. The only distinction is that `wt4117` tests the public
  `wiredtiger_crc32c_func()` API surface while `wt2695` calls internal functions directly; this
  justifies a thin public-API smoke test. If kept, `wt4117` should be reduced to a single-line
  assertion that the function pointer is non-null and returns a correct value for one input.

### [DUP-3] csuite: Two config-parsing/precompile benchmarks

- **Tests involved:** `wt11126_compile_config`, `wt11440_config_check`
- **Overlap:** Both test `begin_transaction` configuration string formatting with four boolean
  parameters and 24 possible combinations. `wt11440` covers variants 0 and 1 (dynamic format vs.
  pre-made table); `wt11126` covers variants 0, 1, 2, 3, and 4, including the new `compile_configuration`/`bind_configuration` API.
- **Recommendation:** `wt11126` is a strict superset of `wt11440` for correctness cases. Variants 0
  and 1 in both tests make identical assertions. `wt11440` could be retired once `wt11126` reaches
  stable status, or `wt11440` could be repurposed as a pure benchmark for the older path.

### [DUP-4] catch2: Version validation duplication between test_disagg_meta_config and ext_test_checkpoint_meta_version

- **Tests involved:** `test_disagg_meta_config` (sections "Parse metadata with version"), `ext_test_checkpoint_meta_version`
- **Overlap:** Both test version/compatible_version field parsing and validation. `test_disagg_meta_config`
  calls `__ut_disagg_parse_version_and_check`; `ext_test_checkpoint_meta_version` calls
  `__ut_disagg_validate_checkpoint_meta_version`. These are two different functions operating on two
  different metadata formats (turtle vs. checkpoint metadata), so the overlap is structural rather
  than exact. The test cases for "missing version", "missing compatible_version", "forward
  incompatibility", and "backward compatible" appear in both with similar logic.
- **Recommendation:** The two functions are tested correctly by separate test files. However, a shared
  test fixture for the common version-field parsing logic would reduce boilerplate and make future
  changes to the version scheme easier to propagate. Low priority.

### [DUP-5] cppsuite: operations_test is a partial subset of bounded_cursor_stress for the non-bounded read path

- **Tests involved:** `operations_test`, `bounded_cursor_stress`
- **Overlap:** `operations_test` runs unbounded insert/read/update/remove with the operation tracker.
  `bounded_cursor_stress` runs the same operations plus bounded cursor reads. The general concurrency
  and MVCC stress coverage overlaps substantially; `operations_test` adds the operation tracker
  validation (which `bounded_cursor_stress` disables) while `bounded_cursor_stress` adds bounded cursor
  correctness.
- **Recommendation:** The two tests serve distinct purposes (tracker-based validation vs. bounded API
  correctness) and should both be retained. The overlap is acceptable given that they provide
  orthogonal correctness guarantees.

---

## Missing Coverage

### [CRITICAL-1] No crash/recovery tests for disaggregated storage (csuite)

**Severity:** Critical. This is the single largest gap in the entire C-level test suite.

**Current state:** All 41 csuite tests use regular B-tree (non-layered) tables. The crash/recovery
tests (random_abort, schema_abort, timestamp_abort) fork a child writer, run a workload, send SIGKILL,
and then verify recovery. None of these tests create a layered table (`block_manager=disagg,type=layered`),
configure a page log extension, or exercise the `block_disagg/` code paths at all.

**Why this is dangerous:** The core crash invariants for disaggregated storage differ fundamentally
from regular B-trees:

1. **No WAL for page log commits.** Regular B-trees use WAL + RTS for durability. Disagg tables
   write pages through the page log (object store) and use a "turtle file" checkpoint mechanism
   (`WT_DISAGG_CHECKPOINT_TURTLE_VERSION`) to mark durable points. A crash between page log writes
   and turtle file update leaves the database in a partially written state that the current
   `test_layered_incomplete_table` test only covers from the metadata perspective, not from the
   data-content perspective.

2. **Drain races.** The ingest table (`file:T.wt_ingest`) accumulates writes; a background drain
   process transfers them to the stable layer (`file:T.wt_stable`). A crash during drain could leave
   both layers partially updated with no WAL-based recovery path.

3. **Role transition races.** When a follower steps up to leader (failover), it replays the last
   stable checkpoint and discards ingest-layer data not yet committed. A crash during this transition
   has no csuite test.

4. **Disagg address cookie integrity.** The `block_disagg_addr.c` cookie packs/unpacks
   page_id/lsn/base_lsn/size/checksum into a version-tagged binary format. A corrupted or partially
   written cookie would silently read wrong pages. There is no crash test that verifies cookie
   integrity after recovery.

5. **Layered table metadata consistency after crash.** `wt4156_metadata_salvage` tests metadata
   salvage for regular tables. There is no equivalent test for layered tables where both
   `file:T.wt_ingest` and `file:T.wt_stable` metadata entries must be written atomically.

**Scenarios that need crash tests:**

- Worker threads writing to a layered table while a checkpoint thread commits stable timestamps;
  SIGKILL at a random point; verify all records up to the last stable checkpoint are readable and
  post-stable records are absent.
- Crash during the ingest-to-stable drain (simulated by a timing stress on the drain path); verify
  that neither layer has dangling data after recovery.
- Crash during leader-to-follower-to-leader transition; verify the new leader sees a consistent
  dataset and does not read from a partially applied checkpoint.
- Crash immediately after writing the turtle file checkpoint but before the page log confirms the
  commit; verify recovery falls back to the previous checkpoint.

---

### [CRITICAL-2] block_disagg internals have zero catch2 unit tests (catch2)

**Severity:** Critical.

**Current state:** The `src/block_disagg/` directory contains 7 source files:
`block_disagg_addr.c`, `block_disagg_ckpt.c`, `block_disagg_mgr.c`, `block_disagg_open.c`,
`block_disagg_read.c`, `block_disagg_unsup.c`, `block_disagg_write.c`. The block/ catch2 tests cover
the parallel regular block manager files extensively (addr, checkpoint, extent lists, session, file
open/close, write). **There are zero catch2 tests for any function in block_disagg.**

**Functions that need unit tests:**

From `block_disagg_addr.c`:
- `__wti_block_disagg_addr_unpack` / `__wti_block_disagg_ckpt_unpack`: Address cookie
  deserialization. These are byte-level binary format functions with version-tagged fields; the
  format is already documented to have upgrade/downgrade debug modes
  (`debug_disagg_address_cookie_upgrade`). Version upgrade/downgrade round-trips have no unit test.
- `__wti_block_disagg_addr_pack` / `__wti_block_disagg_ckpt_pack`: Serialization inverse. A
  pack/unpack round-trip test covering all flag combinations and version modes is absent.
- `__wti_block_disagg_addr_invalid`: Address cookie validity check. Boundary conditions (zero-size,
  null, version mismatch) are not tested.

From `block_disagg_ckpt.c`:
- `__bmd_checkpoint_pack_raw` / `__bmd_checkpoint_unpack_raw`: The checkpoint cookie is what
  survives crashes and is read on recovery. Its correctness under partial writes and version
  mismatches is untested.
- The checkpoint rollback path (`checkpoint_resolve` / `checkpoint_start` error paths): Not tested.

From `block_disagg_write.c`:
- `__wti_block_disagg_write_size`: The size calculation used to allocate write buffers. An off-by-one
  here would silently truncate pages.
- `__wti_block_disagg_header_byteswap_copy`: Currently a no-op placeholder but should have a unit
  test asserting field identity, so that if real byte-swapping is introduced later the test will
  catch any field-level mistakes.

From `block_disagg_mgr.c`:
- The block manager function table (`WT_BM` vtable) initialization: `__bmd_close`, `__bmd_can_truncate`,
  `__bmd_addr_invalid`, `__bmd_block_header` and the remaining stub/forwarding functions have no test
  that verifies they dispatch correctly. The parallel `test_block_file.cpp` tests `__wt_block_open`
  for the regular block manager; there is no equivalent test for `block_disagg_open.c`.

---

### [HIGH-1] No disagg-specific history store stress test (cppsuite)

**Severity:** High.

**Current state:** `hs_cleanup.cpp` runs a workload that ages HS versions by sequentially updating
all keys, advancing timestamps to make old versions globally visible, and verifying that the
checkpoint cleanup server removes them. This test runs only against regular B-tree tables and uses
`general` storage mode. `test_layered25.py` (Python suite) exercises HS interaction with layered
tables but only at the functional API level.

**Why this is a gap:** In disaggregated storage, the history store itself may be a regular B-tree
while the primary data lives in layered tables. The interaction between HS eviction/cleanup and page
log writes is not stress-tested. Specifically:

- When a layered table's MVCC chain is long enough to push old versions to the HS, and the stable
  timestamp advances, the checkpoint cleanup server (cc_pages_removed) must coordinate with the page
  log to know which versions are no longer needed. This coordination is not exercised by either
  `hs_cleanup` or any Python test.
- A disagg HS cleanup variant would run `hs_cleanup`'s update pattern against layered tables
  (configured with `disaggregated=(role=leader,page_log=palite)`), verify that `cache_hs_insert`
  still accumulates, and verify `cc_pages_removed` stays positive, confirming that the cleanup
  subsystem correctly handles the layered table case.

---

### [HIGH-2] No disagg correctness stress test — failover perf test does not assert invariants (cppsuite)

**Severity:** High.

**Current state:** `test_disagg_failover_perf` is the only disagg cppsuite test. It measures
step-up latency as a metric and writes it to a JSON perf file. It does not assert that any record
present before the step-up is visible after the step-up, that no record written after the last stable
checkpoint survived, or that the new leader can write and read data correctly after the role
transition. The test explicitly states: "Pass/fail is determined externally by comparing the output
JSON against a baseline."

**Why this is a gap:** A latency regression test is valuable, but it cannot detect data loss or
correctness violations during failover. A correctness-focused stress test would:

1. Populate `N` collections with `K` keys each in leader mode, advance the stable timestamp, and
   record the last stable checkpoint state (set of {key, value} pairs).
2. Close the connection and reopen as follower.
3. Optionally run a follower workload (writes to the ingest table that will not be committed to
   stable).
4. Step up to leader.
5. Assert: every key from the pre-failover snapshot is readable. Assert: no uncommitted follower
   writes are visible. Assert: new inserts in leader mode complete successfully.

This is a correctness test, not a latency test, and would catch the class of bugs where the
step-up replays the wrong checkpoint or fails to discard ingest-only data.

---

### [HIGH-3] No csuite crash test for checkpoint metadata consistency under concurrent schema operations (csuite)

**Severity:** High.

**Current state:** `schema_abort` tests crash during schema create/drop operations on regular tables
and verifies that orphan metadata entries are not left behind. `wt4156_metadata_salvage` tests
metadata file corruption recovery. Neither test covers the case where a crash occurs while WiredTiger
is writing layered table metadata (both `file:T.wt_ingest` and `file:T.wt_stable` entries, plus the
`table:T` umbrella entry) as three separate metadata operations. If the crash falls between the first
and third write, only a subset of the metadata entries survive, which is the scenario covered by
`test_layered_incomplete_table` in catch2 — but that test injects the incomplete state manually,
not via an actual crash.

**What is missing:** A crash test (schema_abort style, with SIGKILL) that creates and drops layered
tables while a checkpoint thread runs, then verifies no orphan metadata entries exist after recovery.
This would complement `test_layered_incomplete_table` by testing the same invariants via an actual
crash rather than metadata surgery.

---

### [HIGH-4] No unit tests for conn_layered_ingest.c drain logic (catch2)

**Severity:** High.

**Current state:** `/src/conn/conn_layered_ingest.c` implements the drain logic that transfers data
from the ingest layer to the stable layer. This file has been modified recently (it appears in the
git status as `M src/conn/conn_layered_ingest.c`). The drain invariants — that a record present in
the ingest layer appears in the stable layer after drain, that drain is idempotent, that concurrent
drains do not produce duplicates — are not covered by any catch2 test.

**What is missing:** Unit tests for the drain state machine, including:
- Happy-path drain: N records in ingest, drain completes, verify N records in stable.
- Interrupted drain (simulate error return): verify ingest is unchanged and stable is not partially
  updated.
- Idempotency: double-drain of the same ingest records must not corrupt stable.
These tests would use mock page log handles (the pattern already established by `test_page_log_handle.cpp`).

---

### [MEDIUM-1] No crash test for background compact interacting with disagg page log (csuite)

**Severity:** Medium.

**Current state:** The three compact crash tests (`wt8057`, `wt8246`, `wt10897`) all operate on
regular B-tree tables. For disaggregated storage, compaction has different semantics: it operates
on the stable layer (which holds immutable pages from the page log) and may or may not be applicable
to the ingest layer. Background compaction (`wt8246` covers this for regular tables) has no disagg
equivalent.

**Note:** If background compaction is not meaningful for layered tables, this gap should be explicitly
documented and the compaction code paths gated on `!WT_BTREE_IS_LAYERED` assertions. The absence of
a test makes it unclear whether this is an intentional omission or an untested path.

---

### [MEDIUM-2] No csuite test for disagg connection open/close lifecycle (csuite)

**Severity:** Medium.

**Current state:** `wt3120_filesys` tests the custom file system extension load/unload lifecycle for
`fail_fs`. There is no equivalent test for the disaggregated storage extension lifecycle:
`__wti_disagg_conn_config`, `__wti_disagg_destroy`, and `__wti_conn_remove_page_log`. These are
currently only covered by the mock-session catch2 test in `test_page_log_handle.cpp`, which does
not exercise the full connection open/close path.

---

### [MEDIUM-3] No cppsuite test that runs disagg follower under write workload and verifies isolation (cppsuite)

**Severity:** Medium.

**Current state:** `test_disagg_failover_perf` has an optional `-S updates` workload in follower
mode, but it is performance-oriented and does not verify that follower writes to the ingest layer
are correctly isolated from the stable layer visible to a subsequent leader. The Python suite tests
follower cursor operations but does not apply a sustained concurrent write workload.

---

### [LOW-1] block_disagg read path (`block_disagg_read.c`) has no unit test (catch2)

**Severity:** Low.

**Current state:** The regular block manager read path is tested by `block_api_test_block_api_write.md`
(which tests round-trip write+read via the BM API). The disagg read path
(`__wti_block_disagg_read`, `__wti_block_disagg_read_internal`) has no catch2 test. The read path
is exercised by integration tests but not by isolated unit tests that can inject specific page_id
and lsn values and verify the correct bytes are returned.

---

### [LOW-2] LazyFS coverage for crash tests: random_abort has it, but no disagg crash test will have it (csuite)

**Severity:** Low.

**Current state:** `random_abort`, `schema_abort`, and `timestamp_abort` all have LazyFS variants
(`smoke_lazyfs.sh`) that verify recovery after a simulated power failure where only fsync-committed
data survives. If a disagg crash test is added (see CRITICAL-1), it should also have a LazyFS
variant, since the interaction between the page log's write path and fsync semantics is a distinct
failure mode from a simple SIGKILL.

---

## Proposed New csuite Tests for Disagg

### Proposed: `disagg_abort` — Crash/recovery for layered tables (analog of timestamp_abort)

**Priority:** Critical.

**Structure:** Mirror `timestamp_abort/main.c`. Fork a child process that:
1. Opens a WiredTiger connection with `disaggregated=(role=leader,page_log=palite)` and the palite
   extension.
2. Creates one layered table (`block_manager=disagg,type=layered`).
3. Runs N writer threads (5–20) inserting key/value pairs with monotonically increasing commit
   timestamps. Each thread writes a per-thread record file recording every committed (key, timestamp)
   pair.
4. Runs a checkpoint thread that periodically calls `session->checkpoint()` and advances the stable
   timestamp; writes a sentinel file after each completed checkpoint.
5. The parent kills the child after the sentinel file is observed.
6. The parent reopens the database as leader, runs recovery (automatic), and reads each key at its
   commit timestamp.
7. Verification: every key with commit_ts <= last stable_ts must be present; every key with
   commit_ts > last stable_ts must be absent (or present at the stable version only).

**Variants:**
- `-r leader`: Run the recovery phase as leader (default).
- `-r follower`: Reopen as follower after crash; step up to leader; verify.
- `-L`: LazyFS variant — clear the filesystem cache before recovery to simulate power failure.

**Key differences from timestamp_abort:**
- Table creation uses `block_manager=disagg,type=layered` and the palite page log.
- Verification must be aware that logged/unlogged table semantics differ for disagg tables.
- The sentinel file must confirm that at least one stable-timestamp checkpoint was completed via the
  page log (not just a WiredTiger checkpoint file).

---

### Proposed: `disagg_drain_abort` — Crash during ingest-to-stable drain

**Priority:** High.

**Structure:**
1. Child process: populate a layered table with 100,000 records, take a checkpoint (stable
   timestamp T1), then inject a timing stress on the drain path
   (`timing_stress_for_test=[layered_drain]` if such a stress point is added) and initiate another
   drain/checkpoint cycle.
2. Parent kills child during the drain.
3. Parent reopens as leader; verifies that all records up to T1 are present and correct; verifies
   no records from the post-T1 drain attempt are present.

**Dependency:** Requires a timing stress hook in the drain code path. If the drain path does not yet
have a `WT_TIMING_STRESS` injection point, one must be added.

---

### Proposed: `disagg_schema_abort` — Crash during layered table create/drop (analog of schema_abort)

**Priority:** High.

**Structure:** Mirror `schema_abort/main.c`. Fork a child that:
1. Runs threads alternating between: inserting data into a long-lived layered table, and
   creating/dropping short-lived layered tables.
2. Checkpoint thread commits checkpoints and advances stable timestamp.
3. Parent kills child after sentinel file appears.
4. Parent reopens as leader; verifies: long-lived table data is correct up to last stable timestamp;
   short-lived tables are either fully present or fully absent in metadata (no orphan entries); no
   `file:T.wt_ingest` or `file:T.wt_stable` metadata entries exist without a corresponding
   `table:T` entry.

---

## Proposed New catch2 Tests for Disagg

### Proposed: `test_block_disagg_addr` — Address cookie pack/unpack round-trip (analog of block_unit_test_block_addr.md)

**Priority:** Critical.

**File:** `test/catch2/block_disagg/unit/test_block_disagg_addr.cpp`

**Test cases:**

```
TEST_CASE("block_disagg address cookie pack/unpack round-trip") {
    SECTION("standard version — all fields preserved") {
        // Pack a known {page_id, lsn, base_lsn, size, checksum, flags} cookie,
        // unpack it, assert all fields match.
    }
    SECTION("version upgrade (compatible) — readable by older reader") {
        // Set debug_disagg_address_cookie_upgrade = COMPATIBLE.
        // Pack; verify version field is incremented; unpack succeeds.
    }
    SECTION("version upgrade (incompatible) — rejected by older reader") {
        // Set INCOMPATIBLE upgrade. Pack; attempt to unpack with version_min
        // check; verify ENOTSUP is returned.
    }
    SECTION("invalid address — zero size") {
        // __wti_block_disagg_addr_invalid with size=0 returns non-zero.
    }
    SECTION("invalid address — null buffer") {
        // __wti_block_disagg_addr_invalid with null addr returns non-zero.
    }
    SECTION("truncated buffer — unpack returns error") {
        // Pack a full cookie; truncate the buffer by 1 byte; unpack returns error.
    }
}
```

---

### Proposed: `test_block_disagg_ckpt` — Checkpoint cookie pack/unpack (analog of block_unit_test_block_ckpt.md)

**Priority:** Critical.

**File:** `test/catch2/block_disagg/unit/test_block_disagg_ckpt.cpp`

**Test cases:**

```
TEST_CASE("block_disagg checkpoint cookie round-trip") {
    SECTION("empty checkpoint (null root)") {
        // Pack with root_image=NULL; raw.data should be NULL, raw.size=0.
    }
    SECTION("non-empty checkpoint — fields preserved") {
        // Pack a mock root_image with known page_id/lsn/size/checksum.
        // Unpack and verify all WT_BLOCK_DISAGG_ADDRESS_COOKIE fields match.
    }
    SECTION("checkpoint rollback — previous root size restored") {
        // After a failed checkpoint, previous_root_size should be restored to
        // its pre-checkpoint value.
    }
}
```

---

### Proposed: `test_block_disagg_write_size` — Write size calculation

**Priority:** High.

**File:** `test/catch2/block_disagg/unit/test_block_disagg_write_size.cpp`

**Test cases:**

```
TEST_CASE("__wti_block_disagg_write_size") {
    SECTION("normal size — header overhead added correctly") {
        // Input: 4096. Expected: 4096 + WT_BLOCK_DISAGG_HEADER_BYTE_SIZE.
    }
    SECTION("size near overflow — returns EINVAL") {
        // Input: UINT32_MAX - 1000. Should return EINVAL.
    }
    SECTION("zero size — header overhead only") {
        // Input: 0. Output: WT_BLOCK_DISAGG_HEADER_BYTE_SIZE.
    }
}
```

---

### Proposed: `test_block_disagg_open` — Block manager open/close lifecycle (analog of test_block_file.cpp)

**Priority:** High.

**File:** `test/catch2/block_disagg/unit/test_block_disagg_open.cpp`

**Test cases:**

```
TEST_CASE("block_disagg block manager open and close") {
    SECTION("open with palite page log — WT_BM vtable initialized") {
        // Verify all function pointers in WT_BM are non-null after open.
        // Verify bm->block_disagg is non-null.
    }
    SECTION("open with invalid page log name — returns error") {
        // Should return EINVAL or ENOENT.
    }
    SECTION("close after open — succeeds and nulls bm") {
        // __wti_bm_close equivalent for disagg.
    }
}
```

---

### Proposed: `test_disagg_drain` — Ingest drain unit tests

**Priority:** High.

**File:** `test/catch2/misc_tests/test_disagg_drain.cpp`

Using mock page log handles (the pattern from `test_page_log_handle.cpp`):

```
TEST_CASE("Layered table drain") {
    SECTION("happy-path drain — N records transfer from ingest to stable") {}
    SECTION("drain with error return — ingest unchanged, stable not partially updated") {}
    SECTION("idempotent drain — double drain does not corrupt stable") {}
    SECTION("drain statistics — pages_drained counter increments correctly") {}
}
```

---

### Proposed: `test_disagg_conn_lifecycle` — Full connection lifecycle for disagg

**Priority:** Medium.

**File:** `test/catch2/misc_tests/test_disagg_conn_lifecycle.cpp`

Complement `test_page_log_handle.cpp` by testing:

```
TEST_CASE("Disagg connection full lifecycle") {
    SECTION("open as leader, close, reopen as leader — succeeds") {}
    SECTION("open as leader, close, reopen as follower — succeeds") {}
    SECTION("open as follower, step up to leader via reconfigure — succeeds") {}
    SECTION("destroy with active page log handle — no leak") {}
}
```

---

### Proposed: Disagg correctness stress test for cppsuite

**Priority:** High.

**File:** `test/cppsuite/tests/test_disagg_correctness.cpp`

This test should not require the proprietary palite extension (use a lightweight mock page log or
the existing palite extension with a reduced dataset). It should:

1. Populate `M` layered table collections in leader mode with a known dataset.
2. Record a "ground truth" snapshot: for each key, its committed value at stable_ts.
3. Close the connection.
4. Reopen as follower; optionally run a follower write workload.
5. Step up to leader.
6. Scan all collections and verify: every key in the ground truth snapshot is present and has the
   correct value; no key not in the snapshot is present; new insertions after step-up succeed.

**Config files:**
- `test_disagg_correctness_default.txt`: 3 collections, 1,000 keys, no follower workload.
- `test_disagg_correctness_stress.txt`: 20 collections, 50,000 keys, follower append workload.

---

## Summary Table

| Priority | Suite | Gap | Risk |
|---|---|---|---|
| CRITICAL | csuite | No crash/recovery tests for disaggregated storage (zero disagg crash tests) | Data loss after failover goes undetected |
| CRITICAL | catch2 | Zero unit tests for block_disagg/ (7 source files, 0 unit tests) | Address cookie corruption or checkpoint cookie bugs invisible until integration test |
| HIGH | cppsuite | No disagg correctness stress test; failover perf test asserts no invariants | Role-transition data loss not stress-tested |
| HIGH | cppsuite | No disagg HS cleanup stress test | HS/page-log coordination bugs not tested under load |
| HIGH | catch2 | No unit tests for conn_layered_ingest.c drain logic | Drain correctness bugs not caught at unit level |
| HIGH | csuite | No crash test for layered table create/drop (schema_abort analog) | Orphan disagg metadata after crash not tested by actual crash |
| MEDIUM | csuite | No disagg connection open/close lifecycle csuite test | Extension load/unload regressions not caught |
| MEDIUM | cppsuite | No follower-isolation correctness test | Ingest-layer isolation from stable layer not stress-tested |
| MEDIUM | csuite | Compact + disagg interaction unspecified (no test or documented omission) | If compaction is applicable to layered tables, correctness is untested |
| LOW | catch2 | block_disagg read path (`block_disagg_read.c`) not unit tested | Read-path bugs only caught by integration tests |
| LOW | csuite | Future disagg crash tests lack LazyFS variants | Power-failure semantics for page log not validated |
| DUP | csuite | wt4117_checksum is a pure subset of wt2695_checksum | Maintenance overhead, not a safety risk |
| DUP | csuite | wt11440_config_check is subsumed by wt11126_compile_config | Maintenance overhead |
| DUP | catch2 | Version validation logic duplicated across test_disagg_meta_config and ext_test_checkpoint_meta_version | Minor, structural only |
