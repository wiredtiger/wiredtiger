# test_layered77 — Leader-to-follower role transition with split pages in eviction

**File:** `test/suite/test_layered77.py`
**Storage mode:** Disagg/Layered
**Components under test:** Role transition (leader → follower), eviction of split pages, checkpoint split state handling

## Test Cases

### `test_layered77.test_step_down_dirty_eviction`
- **What it tests:** Writes 10,000 rows of 1 KB each (filling the 10 MB cache) with a stable timestamp, checkpoints (creating pages with pending split state), then makes eviction aggressive (`eviction_dirty_target=1, eviction_dirty_trigger=5, eviction_updates_target=1, eviction_updates_trigger=5`). Captures checkpoint metadata, transitions from leader to follower via `reconfigure(role="follower")`, sleeps 0.5s for eviction to process split pages during the transition window, then closes and reopens as follower with the captured checkpoint metadata. Verifies all 10,000 rows are readable via a full scan.
- **Components:** `src/btree/bt_evict.c`, `src/conn/conn_dhandle.c` (role change), `src/btree/bt_page.c` (split state), `src/conn/conn_layered_ingest.c`
- **Notes:** Tests a crash/assertion regression where eviction threads encountering pages with pending split state during a leader→follower role transition would fail. Uses `eviction_util.populate()` and `file:` with `block_manager=disagg` (not `layered:` URI directly). Cache is sized to exactly match the data so eviction pressure is immediate.
