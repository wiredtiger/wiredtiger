# test_truncate28 — Out-of-order commit timestamp rejected for fast truncate

**File:** `test/suite/test_truncate28.py`
**Storage mode:** General
**Components under test:** fast delete, timestamp ordering enforcement, `unexpected timestamp usage` error

## Test Cases

### `test_truncate28.test_truncate28`
- **What it tests:** Inserts 10,000 rows at per-row timestamps; inserts one prepared+committed row at commit_ts=nrows+1, durable_ts=nrows+2 (creating a page with a high durable timestamp); updates stable to nrows//2 and checkpoints; evicts all pages; attempts to fast-truncate from nrows//2 onward with commit_ts=nrows (which is less than the existing durable timestamp on the page) and asserts the commit raises `WiredTigerError` with "unexpected timestamp usage".
- **Components:** `btree.c`, `txn_timestamp.c`, `txn.c`
- **Notes:** Requires non-diagnostic, standalone build (skipped otherwise). Integer-row format only. Tests that the system correctly rejects an out-of-order commit timestamp on a fast truncate, preventing data corruption when commit < durable on existing pages.
