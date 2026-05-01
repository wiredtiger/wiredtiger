# test_layered61 — Ingest table timestamps not cleared when globally visible after eviction

**File:** `test/suite/test_layered61.py`
**Storage mode:** Disagg/Layered
**Components under test:** Ingest table timestamp management, eviction, follower step-up

## Test Cases

### `test_layered61.test_layered61`
- **What it tests:** Verifies that timestamps on ingest table entries are not cleared even when they become globally visible. Inserts a key at commit_timestamp=10, advances oldest_timestamp to 20 (making the entry globally visible), forces page eviction, then steps the node up from follower to leader and reads back the key to confirm data survives.
- **Components:** `src/conn/conn_layered_ingest.c`, `src/btree/bt_evict.c`, `src/cursor/cur_layered.c`
- **Notes:** Disagg-only scenario; runs as follower initially (`role="follower"`) then reconfigures to leader. Uses `debug=(release_evict_page)` to force eviction. The critical invariant is that globally-visible ingest entries must keep their timestamps to avoid data loss across eviction/step-up cycles.
