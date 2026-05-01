# test_cc02 — In-memory vs on-disk obsolete HS content cleanup paths

**File:** `test/suite/test_cc02.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, history store, eviction, statistics

## Test Cases

### `test_cc02.test_cc`
- **What it tests:** Verifies that obsolete history store content is cleaned up through two distinct code paths depending on whether pages are resident in memory or have been evicted to disk. In the eviction (in_memory=True) scenario, CC marks obsolete in-memory pages dirty so they can be evicted, incrementing `checkpoint_cleanup_pages_evict`. In the disk scenario, CC flags the on-disk ref as obsolete, incrementing `checkpoint_cleanup_pages_removed`.
- **Components:** `src/reconcile/`, `src/btree/bt_read.c`, `src/evict/`, `src/history/`
- **Notes:** Two scenarios: `('eviction', in_memory=True)` and `('disk', in_memory=False)`. Disk scenario uses a `debug=(release_evict_page=true)` session to explicitly evict HS pages before making them obsolete. After advancing `oldest_timestamp` to `new_ts=10`, calls `wait_for_cc_to_run()`. Asserts `visited > 0`, `handle_processed > 0`, `inmem_pages_visited > 0`, `duration > 0` (skipped on Windows). Then asserts exactly one of `pages_evict > 0` or `pages_removed > 0` depending on scenario. Also checks `checkpoint_cleanup_duration` and `checkpoint_cleanup_handle_processed` stats.
