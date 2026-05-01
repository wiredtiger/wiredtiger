# test_layered55 — Obsolete time window not reviewed on read-only (follower) btree

**File:** `test/suite/test_layered55.py`
**Storage mode:** Disagg/Layered
**Components under test:** stable btree, eviction, time window management, follower role (read-only btree)

## Test Cases

### `test_layered55.test_obsolete_time_window`
- **What it tests:** Verifies that when a follower's stable btree is read-only, the eviction subsystem does not attempt to review and clean up obsolete time windows (which would be incorrect since the btree cannot be dirtied). Inserts 10,000 rows on the leader with per-row timestamps, takes a checkpoint, reopens as follower, reads all data into cache. Then advances the oldest timestamp to `nrows/2` to make some time windows obsolete, reads again to trigger eviction, and asserts that both the per-btree stat `cache_eviction_dirty_obsolete_tw == 0` and the connection-level stat `cache_eviction_dirty_obsolete_tw == 0`.
- **Components:** stable btree (read-only eviction, no dirty obsolete TW processing), eviction, time window management, follower role
- **Notes:** Uses `eviction_util` mixin for `populate` and `get_stat` helpers. Key format is integer (`i`), value is 1024-byte string. The test is a targeted regression for a correctness bug where the eviction path would incorrectly mark the read-only btree as needing cleanup. Disagg-only.
