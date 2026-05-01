# live_restore — Functional and recovery testing for the live-restore feature

**Path:** `test/live_restore/`
**Storage mode:** General
**Components under test:** live restore (cppsuite `test_live_restore` binary), crash recovery, background restore thread, per-directory database layout, backup cursor

## Overview

The live-restore feature allows WiredTiger to start serving reads and writes from a source backup directory while asynchronously copying files into a new home directory in the background. This test suite exercises that feature through a thin Bash harness (`helper.sh`) that wraps the compiled C++ test binary `test/cppsuite/test_live_restore`. Two shell scripts — `short_test.sh` (quick CI gate) and `long_test.sh` (stress/recovery) — invoke `helper.sh:run_test()` with different flag combinations.

A separate Python utility (`take_backup.py`) produces a pre-populated source backup directory from a wtperf run and is used to set up performance test inputs rather than functional correctness tests.

### helper.sh
Provides a single `run_test()` function that forwards its argument string to the live restore binary. Exit code 137 (SIGKILL, e.g., from `kill -9` during a deliberate crash) is treated as an expected success; any other non-zero exit is a hard failure.

### take_backup.py
Opens the `WT_TEST_0_0` wtperf output directory via a WiredTiger backup cursor, copies every file listed by the cursor into `WT_TEST_0_0_backup`, then closes cleanly. Intended to be run from `build/bench/wtperf` after the `btree-500m-populate` task has completed. This backup becomes the source directory for live-restore performance benchmarks.

---

## Test Scenarios

### Scenario: Short — basic multi-iteration run
- **What it tests:** Runs 10 iterations of live restore with 1 collection and 1000 operations per iteration at log level 2 (INFO). Verifies that the background restore thread completes and data is accessible throughout.
- **Components:** live restore background thread, CRUD operations during restore, single-collection layout
- **Notes:** Entry point for quick CI validation; finishes fast enough to be a pre-merge gate.

### Scenario: Short — crash and recovery
- **What it tests:** Two sub-steps: (1) runs 2 iterations with 10 operations and the `-d` (die/crash) flag, which causes the binary to kill itself mid-run; (2) runs 1 iteration with the `-r` (recovery) flag to open the same database and verify it can recover from the unclean shutdown.
- **Components:** crash/unclean shutdown path, crash recovery, WAL replay during live restore
- **Notes:** Exit code 137 from the crash step is explicitly accepted by `helper.sh`. The recovery step must complete with exit code 0, validating that WT log replay works correctly when restarting a mid-restore database.

### Scenario: Long — high-throughput multi-collection run (flat layout)
- **What it tests:** 10 iterations, 20 collections, 20,000 operations per iteration using the default flat database layout.
- **Components:** live restore background thread, multi-collection scaling, high operation count
- **Notes:** Stress-tests the restore scheduling and locking paths under concurrent CRUD load.

### Scenario: Long — high-throughput multi-collection run (per-directory layout)
- **What it tests:** Same as the flat layout scenario but with the `-D` flag, which enables per-directory database organisation.
- **Components:** per-directory database layout, live restore background thread, multi-collection scaling
- **Notes:** Validates that the file-path mapping logic in live restore works correctly when each table lives in its own subdirectory.

### Scenario: Long — background thread completion wait
- **What it tests:** 2 iterations with the `-b` (background-thread-wait) flag and a 1-second timer (`-t 1`). The binary waits for the background restore thread to fully complete before exiting rather than cutting it off at iteration end. No CRUD operations are applied after startup.
- **Components:** background restore thread lifecycle, thread join/completion signalling
- **Notes:** Specifically targets the thread-completion code path, which is distinct from the normal iteration-exit path. The 1-second timer keeps the total run short.

### Scenario: Long — crash recovery with many operations (flat layout)
- **What it tests:** Two sub-steps: (1) 5 iterations, 50,000 operations, 12-thread parallelism, crash (`-d`); (2) 1 recovery iteration (`-r`) with the same operation count and thread count.
- **Components:** crash recovery, high-concurrency CRUD during live restore, WAL replay
- **Notes:** High thread count (12) and operation count (50k) maximises the chance of crashing while both the restore thread and worker threads are active simultaneously.

### Scenario: Long — crash recovery with many operations (per-directory layout)
- **What it tests:** Same as the flat-layout crash/recovery scenario but with `-D` for per-directory organisation.
- **Components:** per-directory database layout, crash recovery, high-concurrency CRUD during live restore
- **Notes:** Combines the most stressful operational parameters with the per-directory layout to catch file-mapping issues that only appear during recovery.

### Scenario: Backup preparation (`take_backup.py`)
- **What it tests:** Not a correctness test; creates a WiredTiger backup of a pre-populated wtperf database (`WT_TEST_0_0`) using a backup cursor. The resulting directory (`WT_TEST_0_0_backup`) serves as the source directory for live-restore performance benchmarks.
- **Components:** backup cursor API, file enumeration and copy
- **Notes:** Must be run from `build/bench/wtperf` after a wtperf populate run. Not integrated into the short or long test scripts; it is a manual setup step for perf testing.

---

## Coverage Notes

**Uniquely covered:**
- The live-restore background thread is the sole WiredTiger subsystem exercised by this suite; it is not tested anywhere else in the test tree.
- Per-directory database layout (the `-D` flag) is tested specifically for live restore, covering a path in file enumeration and restore scheduling not reachable from the default flat layout.
- The crash-while-restoring path (die then recover) is unique to this suite and exercises the intersection of live restore and crash recovery simultaneously.
- Background-thread-completion signalling (the `-b` flag scenario) targets a lifecycle transition not covered by the iteration-end path.

**Gaps and limitations:**
- All tests delegate to the single binary `test/cppsuite/test_live_restore`; the test scripts have no visibility into which internal assertions are exercised or what data-integrity checks are performed beyond exit-code inspection.
- `take_backup.py` is a manual, one-off utility with no automated invocation; there is no CI job currently linking it to the functional test scripts.
- There is no test for live restore combined with encryption or custom compression.
- The long-test crash/recovery scenarios use a fixed 12-thread count; concurrency levels above or below this value are not exercised.
- There is no negative-path testing (e.g., corrupted source directory, missing files, permission errors).
- Statistics validation is absent; the tests confirm only that the process exits cleanly, not that internal counters reflect expected behaviour.
