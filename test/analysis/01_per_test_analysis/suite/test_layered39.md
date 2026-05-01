# test_layered39 — Eviction is blocked for pages ahead of the materialization frontier

**File:** `test/suite/test_layered39.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, materialization frontier, eviction, page log, checkpoint, precise_checkpoint

## Test Cases

### `test_layered39.test_layered39`
- **What it tests:** Verifies that the eviction subsystem respects the materialization frontier (last materialized LSN) and does not evict pages whose page log writes have not yet been materialized. Uses a 75 MB cache and inserts 200,000 records in batches, interspersed with periodic checkpoints that set the materialization LSN via two different APIs: `page_log.pl_set_last_materialized_lsn(session, lsn)` and `conn.set_context_uint(WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN, lsn)`. After all inserts, triggers explicit eviction of all pages, then asserts: (a) `cache_scrub_restore > 0` (pages were restored because they were ahead of the frontier), (b) `checkpoint_pages_reconciled_bytes > nitems*3*10`, (c) `cache_scrub_restore >= cache_eviction_ahead_of_last_materialized_lsn`. Also verifies that the `last_materialized_lsn` cannot go backwards (raises `WiredTigerError`).
- **Components:** block_disagg (eviction gate on materialization frontier), `pl_get_last_lsn`, `pl_set_last_materialized_lsn`, `WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN`, `conn.reconfigure(disaggregated=(last_materialized_lsn=...))`, checkpoint, eviction, page log, precise_checkpoint=false (not set here)
- **Notes:** 200,000 records, 3 keys per record ("Hello i", "Hi i", "OK i"), 75 MB cache to force eviction pressure. The test validates both the `reconfigure` API path and the `set_context_uint` API path for setting the materialization frontier, and checks that `reconfigure` alone does not persist the LSN across calls. The regression for backwards LSN is also validated. Disagg-only.
