# Checkpoint cleanup partial-delete benchmark

## Goal

Measure whether storing the page-level stop timestamp reduces checkpoint-cleanup reads of
internal pages when a large tree contains sparse, partial deletes. Compare the same deterministic
workload on `develop` and the PR branch. This is a performance benchmark, not a new functional
behavior test.

## Workload

The benchmark creates a standalone table with fixed-size keys and values and small leaf and
internal pages, producing a multi-level tree. It populates the table with timestamped records,
checkpoints, then removes a deterministic sparse subset of keys at a later commit timestamp. The
delete pattern must leave live records in every affected leaf; no leaf should be fully deleted.

Set the oldest timestamp below both the insert and delete timestamps, and the stable timestamp above
the delete timestamp. This persists the deletes in the checkpoint without making them globally
visible. Consequently, the obsolete-time-window path should not force reads for the partial-delete
case. Use fixed-size records and a sparse delete cadence chosen so that each touched leaf retains
live records. Reopen the connection and keep the table handle open before triggering one
checkpoint-cleanup pass. A no-delete case is the control.

## Measurements

Take statistics immediately before and after the forced cleanup pass. Report deltas for internal
and leaf pages read into cache, bytes read into cache, cache bytes, pages visited, and elapsed
cleanup time. Keep the timed interval free of user reads or writes so cache-read deltas primarily
reflect checkpoint cleanup. Report each case in a machine-readable result line and run five
independent repetitions, reporting the median.

The benchmark will validate that cleanup completed and that deleted keys are absent while
survivor keys remain. On the PR branch, also report the obsolete-time-window cleanup-read counter
when available; it should remain zero in the partial-delete case. That check validates that the
benchmark isolates page-aggregate reads rather than counting timestamp cleanup work.

## Comparison and interpretation

Build and run the unchanged benchmark on `develop` and the PR branch with the same build
configuration, record count, cache size, and host; apply only the benchmark change to the baseline
revision. Compare the additional internal reads from partial deletes over the no-delete control on
each branch. The primary signal is the reduction in those internal page reads; bytes read, cache
growth, and cleanup duration indicate its cost or benefit. WiredTiger cache statistics measure
pages loaded into its cache, not physical device I/O. The benchmark does not claim a universal
workload benefit.

## Scope

Add a standalone benchmark target under `bench/checkpoint_cleanup`, following existing benchmark
and testutil conventions. Do not add production statistics or change checkpoint-cleanup behavior
as part of the benchmark. Correctness coverage remains in the suite tests already present on the
PR branch.
