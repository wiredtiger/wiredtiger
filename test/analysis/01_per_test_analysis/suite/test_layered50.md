# test_layered50 — Follower can evict pages without setting materialization frontier

**File:** `test/suite/test_layered50.py`
**Storage mode:** Disagg/Layered
**Components under test:** stable btree, eviction, checkpoint pick-up, follower role

## Test Cases

### `test_layered50.test_evict_on_standby`
- **What it tests:** Verifies that a follower node (which does not set the materialization frontier) can evict stable-btree pages without error. Inserts data on the leader, takes a checkpoint, advances it to the follower, then force-evicts all pages on the follower using a debug cursor. Asserts `cache_eviction_clean > 0` to confirm eviction actually occurred.
- **Components:** stable btree (read-only eviction on follower), eviction (`debug=(release_evict_page=true)`), checkpoint pick-up, follower role
- **Notes:** 10 items. Uses a 10 MB cache. No materialization frontier is set — the key scenario being tested is that the follower can evict pages from the stable btree without the frontier constraint (which only applies to pages pending materialization on the leader). Disagg-only.
