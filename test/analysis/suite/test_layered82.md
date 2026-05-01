# test_layered82 — Cursor bounds on layered cursors

**File:** `test/suite/test_layered82.py`
**Storage mode:** Disagg/Layered
**Components under test:** Cursor bounds (`bound=lower/upper`, `inclusive=true/false`, `action=clear`), layered cursor iteration, ingest-only, stable-only, interleaved data layouts

## Test Cases

### `test_layered82.test_bounds_ingest_only`
- **What it tests:** All 1000 keys written locally (no checkpoint). Bounds [200, 800] forward scan returns keys 200–800 exactly.
- **Components:** `src/cursor/cur_layered.c`, ingest btree bound enforcement

### `test_layered82.test_bounds_stable_only`
- **What it tests:** All 1000 keys checkpointed. Bounds [200, 800] forward scan returns keys 200–800 exactly.
- **Components:** Stable btree bound enforcement

### `test_layered82.test_bounds_split_data`
- **What it tests:** Interleaved: even keys in stable, odd keys in ingest. Bounds [200, 800] forward scan returns all keys 200–800.
- **Components:** Merge of stable and ingest sub-cursors within bounds

### `test_layered82.test_bounds_split_data_prev`
- **What it tests:** Interleaved data. Bounds [200, 800] backward scan returns keys 800–200 in reverse order.
- **Components:** `prev` direction with bounds on interleaved data

### `test_layered82.test_bounds_lower_only`
- **What it tests:** Interleaved data. Lower bound only at 500. Forward scan returns keys 500–999.
- **Components:** Lower bound without upper bound

### `test_layered82.test_bounds_upper_only`
- **What it tests:** Interleaved data. Upper bound only at 500. Forward scan returns keys 0–500.
- **Components:** Upper bound without lower bound

### `test_layered82.test_bounds_exclusive_lower`
- **What it tests:** Interleaved data. Exclusive lower bound at 200 (`inclusive=false`). Forward scan returns keys 201–999.
- **Components:** Exclusive lower bound filtering

### `test_layered82.test_bounds_exclusive_upper`
- **What it tests:** Interleaved data. Exclusive upper bound at 800. Forward scan returns keys 0–799.
- **Components:** Exclusive upper bound filtering

### `test_layered82.test_bounds_both_exclusive`
- **What it tests:** Both bounds exclusive: (200, 800). Forward scan returns keys 201–799.
- **Components:** Both exclusive bounds combined

### `test_layered82.test_bounds_nonexistent_keys`
- **What it tests:** Even keys only (stable). Bounds [201, 799] (both bounds on non-existent odd keys). Forward scan returns even keys 202–798.
- **Components:** Bounds at non-existent key positions

### `test_layered82.test_bounds_no_data_in_range`
- **What it tests:** Interleaved data (keys 0–999). Bounds [1500, 2000] beyond all data. Forward scan returns empty list.
- **Components:** Empty range detection

### `test_layered82.test_bounds_tombstone_inside`
- **What it tests:** All keys stable. Removes every 3rd key in [200, 800] from ingest. Bounds [200, 800] forward scan returns only non-deleted keys.
- **Components:** Tombstone skip within bounded range

### `test_layered82.test_bounds_tombstone_at_bounds`
- **What it tests:** All keys stable. Removes keys 200 and 800 (the bound keys). Bounds [200, 800] (inclusive). Forward scan returns keys 201–799.
- **Components:** Tombstones at exact bound endpoints

### `test_layered82.test_bounds_all_tombstoned_in_range`
- **What it tests:** All keys stable. Removes all keys 200–800. Bounds [200, 800]. Forward scan returns empty list.
- **Components:** All-tombstoned bounded range

### `test_layered82.test_bounds_tombstone_outside`
- **What it tests:** All keys stable. Removes all keys outside [200, 800]. Bounds [200, 800]. Forward scan returns intact keys 200–800.
- **Components:** Tombstones outside bounds do not affect the bounded scan

### `test_layered82.test_bounds_clear`
- **What it tests:** Sets bounds [200, 800], scans (returns 601 keys), then calls `cursor.bound("action=clear")` and re-scans to confirm all 1000 keys are returned.
- **Components:** `bound(action=clear)` on layered cursor

### `test_layered82.test_bounds_search_near`
- **What it tests:** Bounds [300, 700]. `search_near(100)` (below lower bound) returns key=300 with exact=1.
- **Components:** `search_near` with lower bound violation clamping

### `test_layered82.test_bounds_search_near_upper`
- **What it tests:** Bounds [300, 700]. `search_near(900)` (above upper bound) returns key=700 with exact=-1.
- **Components:** `search_near` with upper bound violation clamping

### `test_layered82.test_bounds_set_before_data`
- **What it tests:** Sets bounds [200, 800] before any data is inserted, scans (returns empty). Then inserts 1000 ingest keys and opens a new bounded cursor, scans (returns 601 keys).
- **Components:** Bounds set on an empty table, then re-queried after data insertion

### `test_layered82.test_bounds_ingest_overrides_stable`
- **What it tests:** All keys stable. Local ingest write overrides key=500 with "new_500". Bounds [500, 500] (single-point). `next()` returns key=500 with value="new_500", then returns `WT_NOTFOUND`.
- **Components:** Ingest value precedence over stable within bounds

### `test_layered82.test_bounds_adjacent_exclusive`
- **What it tests:** Interleaved data. Exclusive bounds (199, 201) with both sides exclusive. Forward scan returns only key=200.
- **Components:** Adjacent exclusive bounds isolating a single key

### `test_layered82.test_bounds_single_point`
- **What it tests:** Interleaved data. Bounds [500, 500] (both inclusive). Forward scan returns only key=500. Backward scan also returns only key=500.
- **Components:** Single-point bounds in both directions

### `test_layered82.test_bounds_search`
- **What it tests:** Interleaved data. Bounds [200, 800]. `search(500)` inside bounds returns 0. `search(100)` below lower bound returns `WT_NOTFOUND`. `search(900)` above upper bound returns `WT_NOTFOUND`.
- **Components:** Bounds enforcement in `search()` operation

### `test_layered82.test_bounds_rebind`
- **What it tests:** Interleaved data. Bounds [0, 999] full scan returns all 1000 keys. Reset, rebind to [400, 600]. Scan returns keys 400–600.
- **Components:** Rebinding bounds after reset, without explicit `action=clear`

### `test_layered82.test_bounds_positioned_update_mid_scan`
- **What it tests:** Interleaved data. Bounds [200, 800]. Scans forward 100 keys, then performs a positioned update (writes "updated" to current key via `cursor.update()`). Continues scanning. Verifies all returned keys are in strict ascending order and all remain within [200, 800].
- **Components:** Write during active bounded scan, cursor position stability
- **Notes:** Tests that a mid-scan update does not corrupt iteration order or break the bound enforcement.
