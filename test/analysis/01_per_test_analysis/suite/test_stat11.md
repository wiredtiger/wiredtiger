# test_stat11 — Presence check for eviction-blocked statistics

**File:** `test/suite/test_stat11.py`
**Storage mode:** General
**Components under test:** eviction statistics keys

## Test Cases

### `test_stat11.test_stats_exist`
- **What it tests:** Opens a connection-level statistics cursor and confirms that each of the six `cache_eviction_blocked_*` stats exists (returns a non-None value): `cache_eviction_blocked_checkpoint`, `cache_eviction_blocked_hazard`, `cache_eviction_blocked_internal_page_split`, `cache_eviction_blocked_overflow_keys`, `cache_eviction_blocked_recently_modified`, `cache_eviction_blocked_uncommitted_truncate`.
- **Components:** `stat.c`, `evict.c`
- **Notes:** Does not verify stat values, only existence. All statistics enabled.
