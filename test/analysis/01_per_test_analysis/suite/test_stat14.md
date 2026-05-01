# test_stat14 — Eviction threshold statistics accuracy

**File:** `test/suite/test_stat14.py`
**Storage mode:** General
**Components under test:** eviction threshold statistics (multiplied by 100 for precision)

## Test Cases

### `test_stat14.test_eviction_threshold_stats`
- **What it tests:** Reads default eviction threshold stats and verifies expected values (eviction_target=8000, trigger=9500, dirty_target=500, dirty_trigger=2000, updates_target=250, updates_trigger=1000); then uses `conn.reconfigure()` to change each threshold individually and verifies the corresponding stat reflects the new value multiplied by 100.
- **Components:** `stat.c`, `evict.c`, `conn.c`
- **Notes:** The `*100` scaling stores two decimal places of precision (e.g. 2.5% → 250). Default `eviction_updates_target` is auto-set to `dirty_target/2 = 2.5%`. Tests integer values: 70%, 85%, 10%, 25%, 8%, 15% for the six thresholds.
