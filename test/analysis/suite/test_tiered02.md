# test_tiered02 — Tiered table checkpoint and flush_tier with growing datasets

**File:** `test/suite/test_tiered02.py`
**Storage mode:** Tiered
**Components under test:** flush_tier (checkpoint), local object creation, connection reopen/recovery, tiered metadata, SimpleDataSet, ComplexDataSet

## Test Cases

### `test_tiered02.test_tiered`
- **What it tests:** End-to-end lifecycle of a tiered table across multiple populate-checkpoint-flush-reopen cycles. Verifies that data written before flush is visible after connection restart, that flushed objects accumulate in the bucket on each explicit `flush_tier`, and that a plain `checkpoint()` (without `flush_tier`) does NOT increase the object count in the bucket.
- **Components:** `src/tiered/` flush path, `conn_tiered.c` checkpoint integration, `storage_sources/dir_store`, metadata persistence across reopens
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store) AND dataset types (SimpleDataSet vs ComplexDataSet).
  - Object-count verification (`confirm_flush`) is skipped for non-dir_store backends because directory listing is not uniformly available.
  - Exercises a known flakiness path (WT-7639): if the object count does not increase immediately after flush, the test retries up to 10 times with exponential back-off.
  - Stages: 10 rows → flush; reopen; 50 rows → flush (with open cursor); 100 rows → flush; 200 rows → close+reopen; 300 rows → plain checkpoint only (expect no new objects).
  - Key format is `key_format=S` for both dataset types.
