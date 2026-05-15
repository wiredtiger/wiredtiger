# C3 Investigation: compact's own allocation path

**Date:** 2026-05-14  
**Ticket:** WT-14196  
**Question:** Should compact's own block relocations use first-fit or restricted best-fit?

## Background

C2 implemented three-branch routing in `__wti_block_alloc`:
- threshold == 0 → pure best-fit (no compact running)
- old_offset >= threshold → first-fit within [0, threshold) for compact's own relocations
- old_offset < threshold or INVALID → restricted best-fit within [0, threshold) for concurrent writes

The C3 investigation asked whether Branch 1 (first-fit for compact) is actually necessary, or whether
restricted best-fit produces equivalent compaction quality at lower cost.

## Measurements

Benchmark: `compact-stress-4t.wtperf` (4 update threads, 1M rows, 1/3 removed, run_time=300s,
compact_ends_workload=true, force=true). Ran twice in sequence, each with a fresh home directory.

| Variant | Throughput (ops/sec) | File size reduction |
|---------|----------------------|---------------------|
| C2: first-fit for compact (Branch 1 active) | 40,792 | 54.23% |
| C3 Branch A: restricted best-fit for compact | 72,776 | 54.16% |

Throughput improvement: +78% (40,792 → 72,776 ops/sec).  
File-size delta: −0.07 percentage points (54.23% → 54.16%).

Routing counters during C3 run:
- `block_alloc_restricted_best_fit_count`: all allocations under compact used restricted best-fit
- `block_first_srch_walk_time_max_usecs`: ~0 (no first-fit walks at all)

## Decision

Branch A wins. Both decision criteria from the plan were met:

1. **Throughput gate**: ≥39,976 ops/sec (95% of baseline) → achieved 72,776 ops/sec.
2. **File-size floor**: ≥50% reduction → achieved 54.16%.

The 0.07 pp compaction quality difference is negligible. First-fit's advantage (packing blocks
at the lowest possible offset) is not observable in practice because best-fit within the low
region achieves essentially the same fill pattern when the avail list has many small extents.

## Implementation

Branch 1 (first-fit) deleted from `__wti_block_alloc`. The `old_offset` parameter is preserved
in the function signature (retained for API compatibility and potential future use) but is not
consulted — marked `WT_UNUSED(old_offset)`. The routing is now:

- threshold == 0 → best-fit (full avail list)
- threshold > 0 → restricted best-fit [0, threshold), fallback to full best-fit then extend

`block_alloc_first_fit_count` stat removed (never non-zero in final design).

## Files changed in C3

- `src/block/block_ext.c`: deleted first-fit branch; updated routing comment to two-branch
- `dist/stat_data.py`: removed `block_alloc_first_fit_count`
- `src/include/stat.h`, `src/include/wiredtiger.h.in`, `src/support/stat.c`: regenerated
- `bench/wtperf/wtperf.c`: removed first-fit stat cursor read and fprintf
- `bench/perf_run_py/perf_stat_collection.py`: removed first-fit PerfStat entry
- `test/catch2/block/unit/test_block_alloc_threshold.cpp`: updated test and comments to reflect
  two-branch design; renamed "takes first-fit branch" test to "still uses restricted-best-fit"
