# test_tiered11 — Flush timestamp and flush time recorded in tiered metadata

**File:** `test/suite/test_tiered11.py`
**Storage mode:** Tiered
**Components under test:** flush_timestamp and flush_time fields in tiered/object metadata, stable timestamp interaction with flush_tier, metadata cursor inspection

## Test Cases

### `test_tiered11.test_tiered11`
- **What it tests:** Verifies that `flush_tier` records the correct stable timestamp (the stable TS at checkpoint time, not at flush time) and a non-zero flush wall-clock time in the metadata of both the `tiered:` and `object:` URIs. Sequence: (1) create tiered table with integer key/value format; (2) add 10 rows with commit timestamps and advance stable/oldest timestamps; (3) take a plain checkpoint — stable TS is `end_ts`; (4) add 10 more rows and advance stable TS again to a new value; (5) call `checkpoint('flush_tier=(enabled)')` — the flush should record `end_ts` (from the previous checkpoint), not the newer stable TS; (6) take another plain checkpoint; (7) inspect metadata on `tiered:test_tiered11` and `object:test_tiered11-0000000001.wtobj`: assert `flush_timestamp="<end_ts>"` is present and `flush_time=0` is absent (i.e., flush_time is non-zero).
- **Components:** `src/tiered/conn_tiered.c` (flush timestamp recording), `src/meta/meta_ckpt.c`, metadata cursor (`metadata:`), stable timestamp management
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store).
  - Commit timestamps use `i * 2` to leave room between commit and stable timestamps.
  - The distinction tested: the flush_timestamp is the stable TS from the immediately preceding checkpoint, not from any later modification.
