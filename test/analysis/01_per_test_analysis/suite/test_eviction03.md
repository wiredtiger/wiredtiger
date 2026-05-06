# test_eviction03 — Disk footprint reduction after obsolete time window cleanup via eviction

**File:** `test/suite/test_eviction03.py`
**Storage mode:** General (requires diagnostic build)
**Components under test:** eviction, time window cleanup, block manager, wt verify utility

## Test Cases

### `test_eviction03.test_eviction03`
- **What it tests:** Creates 3 tables of 10,000 rows each (1 KB values), sets a pinned oldest timestamp, populates with per-key timestamps, closes (checkpointing to disk). Dumps page-level disk sizes with `wt verify -d dump_pages` and computes the average per-page size before cleanup. Reopens with `eviction_obsolete_tw_pages_dirty_max=10000`, advances oldest timestamp to make all TW info obsolete, runs eviction via debug cursor, reopens again, re-dumps, and asserts that the average on-disk page size is smaller after cleanup. Confirms that removing obsolete time window information actually reduces the physical disk footprint.
- **Components:** `src/evict/`, `src/block/`, `src/reconcile/`, `src/utilities/` (wt verify)
- **Notes:** Requires `wiredtiger.diagnostic_build()` (test is skipped otherwise). Uses `eviction_util.evict_cursor_tw_cleanup` and `suite_subprocess.runWt`. Parses `dsk_mem_size` from `verify -d dump_pages` output using regex. Checks `avg_disk_footprint_values[i] > avg_disk_footprint` after cleanup.

### Eviction trigger
- `debug=(release_evict)` cursor scans all pages. The heuristic limit is set to 10,000 to maximize cleanup. Correctness property: average disk size per page strictly decreases.
