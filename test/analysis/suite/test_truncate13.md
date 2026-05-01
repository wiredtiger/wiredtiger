# test_truncate13 — Reading in gaps created by fast delete at various timestamp visibility levels

**File:** `test/suite/test_truncate13.py`
**Storage mode:** General
**Components under test:** fast delete, timestamp visibility, history store, cursor scan across fast-deleted gaps

## Test Cases

### `test_truncate13.test_truncate`
- **What it tests:** Writes 10,000 rows at ts=20 (valuea) and ts=30 (valueb); evicts all pages; sets stable=25; checkpoints; fast-truncates either the start, middle, or end half of the table at ts=35; optionally makes the truncation stable (stable=40) and/or oldest (oldest=40); checkpoints again; optionally writes new data at ts=45; verifies that reading before the truncation (ts=20, ts=30) sees all rows, reading after the truncation sees the expected half remaining, and if new data was written it can all be read back.
- **Components:** `btree.c`, `txn_timestamp.c`, `history_store.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × start/middle/end truncation position × unstable/stable/visible timestamp advancement × add/noadd data (48 scenarios). Skipped on disagg if fast truncate not built. Tests correct cursor navigation across namespace gaps created by fast delete at multiple levels of timestamp stability.
