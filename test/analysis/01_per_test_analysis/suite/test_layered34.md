# test_layered34 — Materialization frontier controls eviction of pages

**File:** `test/suite/test_layered34.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, page log, materialization frontier (`pl_set_last_materialized_lsn`, `last_materialized_lsn`), eviction, checkpoint, stable btree

## Test Cases

### `test_layered34.test_layered34`
- **What it tests:** Verifies that the materialization frontier correctly restricts which pages may be evicted. Creates a single table, writes data in two checkpoints, sets the materialization LSN to the first checkpoint's LSN, then force-evicts a page using a debug cursor. Verifies that the page can still be read back correctly — i.e., the evicted page is re-fetched from the page log at or before the frontier rather than from a newer (non-materialized) LSN.
- **Components:** page log (`pl_get_last_lsn`, `pl_set_last_materialized_lsn`), `last_materialized_lsn` connection reconfigure, block_disagg (eviction gate), checkpoint (precise_checkpoint enabled), stable btree
- **Notes:** Uses a `table:` URI with `block_manager=disagg` (the "shared" scenario). Starts as follower, then steps up to leader. Uses `debug=(release_evict)` cursor to trigger eviction of a specific page. The scenario parameter list contains only a single entry (`shared`). Tests the API `conn.get_page_log(...)` and `page_log.terminate(session)`. Disagg-only, precise_checkpoint=true.
