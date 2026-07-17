# Checkpoint Cleanup Cache Pollution Benchmark

## Goal

Provide a repeatable standalone C microbenchmark demonstrating the WT-13076
behavioral improvement: deleting one key should not cause checkpoint cleanup to
read unrelated internal pages as though an entire page had been deleted.

## Existing Conventions

The benchmark follows the existing `bench/dhandle` and `bench/wt2853_perf`
patterns:

- Use `test_util.h` for WiredTiger setup, option parsing, and cleanup.
- Use `bench_timer.h` for elapsed-time measurement.
- Register the executable with a local `CMakeLists.txt` and
  `create_test_executable`.
- Recreate a private home directory by default and support the shared `-h` and
  `-p` options.

## Workload

The executable creates a row-store table with small leaf pages and enough
records to form multiple internal levels. It runs two equivalent cases:

1. Build and checkpoint the tree, advance the oldest timestamp, and run the
   checkpoint-cleanup cycle without deleting a key.
2. Rebuild the same tree, delete exactly one key in a known leaf, advance the
   oldest timestamp, and run the same checkpoint-cleanup cycle.

Each case starts from a fresh home directory or a fresh table so cache and
statistics counters are not contaminated by the other case. The benchmark
verifies that the deleted key is absent and neighboring keys remain readable.

## Measurements

The benchmark opens a statistics cursor before and after the cleanup cycle and
reports deltas for:

- `cache: internal pages read into cache`.
- `cache: bytes read into cache`.
- `cache: bytes currently in the cache`.
- `btree-size: internal page bytes`.
- Cleanup elapsed time and completed operations.

Output includes a machine-readable result line containing the case name and all
configuration values. Timing is diagnostic only; the benchmark does not impose
a machine-dependent pass/fail threshold.

## Comparison Method

Run the same executable and parameters on the pre-merge parent and the merged
branch. Compare the one-delete-minus-no-delete deltas. The expected result on
the merged branch is fewer unnecessary internal-page reads and fewer bytes read
for the single-delete case, while correctness checks remain unchanged.

## Validation

- Configure and build the benchmark with CMake.
- Run a small deterministic workload locally.
- Run a larger workload that creates several internal levels.
- Confirm no generated files or benchmark data are added to the worktree.
