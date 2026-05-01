# test_compact02 — Compaction reduces file size; dryrun estimation mode

**File:** `test/suite/test_compact02.py`
**Storage mode:** General
**Components under test:** compaction subsystem, block manager, dryrun estimation

## Test Cases

### `test_compact02.test_compact02`
- **What it tests:** Verifies that foreground compaction reduces the on-disk file size below 50% of the pre-compaction size after deleting the even-indexed half of 22 000 records (alternating big 9 KB and small 2.7 KB values). Also tests `dryrun=true` mode which estimates work without modifying the file.
- **Components:** `src/block/block_compact.c`, `src/session/session_compact.c`
- **Notes:** Cross-product of 12 scenarios: types (table) × cacheSize (default, 1 MB, 10 GB) × fileConfig (default, 8 KB, 64 KB, 128 KB leaf pages) × dryrun (true/false). Sets `leaf_value_max=10MB` to prevent overflow items. Retries up to 100 times on `EBUSY` (eviction collision) with 6-second waits. For non-dryrun non-tiered: asserts `sz < fullsize // 2` and stat invariant. For dryrun with ≥1000 pages reviewed: asserts `bytes_rewritten_expected > 0` and `pages_rewritten_expected > 0`. Custom connection setup overrides standard setUp methods to vary cache size per scenario.
