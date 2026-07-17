# Checkpoint Cleanup Cache Pollution Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standalone C microbenchmark that compares checkpoint-cleanup internal-page reads for a cold-cache tree with no deletion versus exactly one deleted key.

**Architecture:** Add one executable under `bench/checkpoint_cleanup` following `bench/dhandle`: shared test utilities, a benchmark timer, private database homes, and a local CMake target. Each case populates and checkpoints a multi-level row-store tree, closes and reopens to empty the WT cache, performs the optional single-key deletion and checkpoint, closes and reopens again, then measures cleanup statistics and validates data.

**Tech Stack:** C, WiredTiger C API, `test_util.h`, `bench_timer.h`, CMake.

---

### Task 1: Preserve and validate the merge fix

**Files:**
- Modify: `src/include/btree_inline.h:2867-2871`

- [ ] **Step 1: Stage the corrected visibility field**

Use the page-level durable timestamp field that exists in the WT-13076 aggregate:

```c
      __wt_txn_snap_range_visible(session, ta->oldest_stop_txn, ta->newest_stop_txn,
        ta->newest_stop_ts, ta->newest_page_stop_durable_ts)) {
```

- [ ] **Step 2: Build the existing CMake tree**

Run:

```bash
cmake --build /tmp/wt-13076-merge-build -j2
```

Expected: the build completes without the previous `newest_stop_durable_ts` member error.

- [ ] **Step 3: Commit the merge-resolution correction**

```bash
git add src/include/btree_inline.h
git commit -m "Fix page stop timestamp in range visibility check"
```

### Task 2: Add benchmark target scaffolding

**Files:**
- Create: `bench/checkpoint_cleanup/CMakeLists.txt`
- Create: `bench/checkpoint_cleanup/bench_checkpoint_cleanup.c`
- Modify: `CMakeLists.txt` at the benchmark subdirectory registration

- [ ] **Step 1: Register the target**

Add a POSIX-only target matching `bench/dhandle/CMakeLists.txt`:

```cmake
project(C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

if(WT_POSIX)
  create_test_executable(bench_checkpoint_cleanup
    SOURCES
    bench_checkpoint_cleanup.c
    ../dhandle/bench_timer.c
    )
endif()
```

Add `add_subdirectory(bench/checkpoint_cleanup)` beside the existing benchmark subdirectories.

- [ ] **Step 2: Add executable option parsing**

Use `TEST_OPTS` and `testutil_parse_opts`, with benchmark-specific options for record count, cache size, leaf page size, and seed. Defaults must be deterministic and small enough for local validation:

```c
#define DEFAULT_RECORDS (200 * WT_THOUSAND)
#define DEFAULT_CACHE_SIZE "64MB"
#define DEFAULT_LEAF_PAGE_MAX "4KB"

static int
usage(void)
{
    fprintf(stderr, "usage: %s [-h home] [-p] [-n records] [-c cache_size]\n", progname);
    return (EXIT_FAILURE);
}
```

- [ ] **Step 3: Configure the target**

Run:

```bash
cmake -S . -B /tmp/wt-13076-merge-build -DWITH_PYTHON=OFF
cmake --build /tmp/wt-13076-merge-build --target bench_checkpoint_cleanup -j2
```

Expected: the executable links successfully.

### Task 3: Implement cold-cache workload and measurements

**Files:**
- Modify: `bench/checkpoint_cleanup/bench_checkpoint_cleanup.c`

- [ ] **Step 1: Implement the case lifecycle**

Implement `run_case(bool delete_one, const OPTIONS *, RESULT *)` with this exact lifecycle:

```c
open_connection(home, cache_size, "statistics=(all)");
create_table("table:cleanup", "key_format=Q,value_format=S,leaf_page_max=4KB");
populate_records(record_count);
checkpoint();
close_connection();

open_connection(home, cache_size, "statistics=(all),checkpoint_cleanup=(wait=1)");
if (delete_one) {
    delete_key(record_count / 2);
    checkpoint();
    close_connection();
    open_connection(home, cache_size, "statistics=(all),checkpoint_cleanup=(wait=1)");
}
reset_statistics_baseline();
run_cleanup_checkpoint();
read_statistics_delta(result);
verify_deleted_or_neighbor_keys(delete_one);
close_connection();
```

The implementation must use a fresh home directory for each case and must not reuse the connection that populated the tree for the measured cleanup phase.

- [ ] **Step 2: Implement statistics extraction**

Open a statistics cursor with `statistics:` and read these keys by their exact WiredTiger statistic names:

```c
static const char *STAT_INTERNAL_READS = "cache: internal pages read into cache";
static const char *STAT_BYTES_READ = "cache: bytes read into cache";
static const char *STAT_CACHE_BYTES = "cache: bytes currently in the cache";
static const char *STAT_INTERNAL_BYTES = "btree-size: internal page bytes";
```

Store before and after values in a `RESULT` structure and print:

```text
RESULT case=no_delete records=<n> internal_reads=<n> bytes_read=<n> cache_bytes=<n> internal_bytes=<n> elapsed_us=<n>
RESULT case=one_delete records=<n> internal_reads=<n> bytes_read=<n> cache_bytes=<n> internal_bytes=<n> elapsed_us=<n>
```

- [ ] **Step 3: Implement correctness checks**

After cleanup, assert that the deleted key returns `WT_NOTFOUND` and that its immediate predecessor and successor return `0`. The no-delete case must return `0` for all three probes.

- [ ] **Step 4: Run the small workload**

Run:

```bash
/tmp/wt-13076-merge-build/bench/checkpoint_cleanup/bench_checkpoint_cleanup -n 20000
```

Expected: two `RESULT` lines, successful key checks, and a lower or equal one-delete internal-read delta on the merged branch than on the pre-merge parent.

### Task 4: Validate comparison and repository state

**Files:**
- Test: `/tmp/wt-13076-merge-build/bench/checkpoint_cleanup/bench_checkpoint_cleanup`

- [ ] **Step 1: Run a larger multi-level workload**

```bash
/tmp/wt-13076-merge-build/bench/checkpoint_cleanup/bench_checkpoint_cleanup -n 500000 -c 64MB
```

Expected: the tree has multiple internal levels and the one-delete case reports the internal-page read and byte deltas.

- [ ] **Step 2: Compare against the pre-merge parent**

Build and run the same target from `HEAD^2` or the pre-merge branch parent with identical arguments, then compare the two `RESULT` pairs. Do not use timing as a pass/fail threshold; use the statistics deltas to demonstrate the behavior.

- [ ] **Step 3: Check generated files and whitespace**

```bash
git diff --check
git diff --name-only --diff-filter=U
```

Expected: no unresolved files, no benchmark database left in the repository, and only intended source/CMake changes.

- [ ] **Step 4: Commit the benchmark**

```bash
git commit -m "Add checkpoint cleanup cache pollution benchmark"
```
