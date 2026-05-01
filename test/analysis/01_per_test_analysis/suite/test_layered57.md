# test_layered57 — Follower does not use application threads for evicting dirty/update pages

**File:** `test/suite/test_layered57.py`
**Storage mode:** Disagg/Layered
**Components under test:** ingest btree, eviction (follower application thread skip), cur_layered.c

## Test Cases

### `test_layered57.test_follower_not_do_app_evict`
- **What it tests:** Verifies that a follower node does not use application threads to evict pages that have updates or are dirty (which would be unsafe since followers write only to the ingest table). Inserts 1000 records with large random keys and values (1000 characters each) into a layered table on a follower, filling the 10 MB cache. After inserts, checks that `cache_eviction_app_threads_skip_updates_dirty_page > 0`, confirming that application threads correctly skipped dirty/update pages during eviction attempts.
- **Components:** ingest btree (dirty-page eviction guard on follower), eviction (`cache_eviction_app_threads_skip_updates_dirty_page` stat), cur_layered.c
- **Notes:** The connection is started as `role="follower"` from the beginning (no leader connection in this test). 1000-character random keys and values ensure rapid cache pressure. Disagg-only.
