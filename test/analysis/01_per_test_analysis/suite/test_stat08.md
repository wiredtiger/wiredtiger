# test_stat08 — Session statistics: bytes read and dirty byte tracking

**File:** `test/suite/test_stat08.py`
**Storage mode:** General
**Components under test:** session statistics (`bytes_read`, `read_time`, `txn_bytes_dirty`), evict, cache

## Test Cases

### `test_stat08.test_session_stats`
- **What it tests:** Inserts 100,000 records in batches with explicit transactions, verifying after each insert that `txn_bytes_dirty` increases; checks that `txn_bytes_dirty` never exceeds the connection's `cache_bytes_dirty`; after commit and full scan verifies `session.bytes_read > 0` and `session.read_time > 0`. Also verifies that `stat_cursor.reset()` zeroes all session stats.
- **Components:** `stat.c`, `session.c`, `txn.c`, `cache.c`, `evict.c`
- **Notes:** Uses `debug=(release_evict_page=true)` to force pages out of cache so reads-from-disk occur. On Windows, skips `read_time` check due to timer granularity. Rolls back and restarts transactions every 200 operations to avoid excessive dirty footprint.
