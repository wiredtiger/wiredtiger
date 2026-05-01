# test_txn25 — Write generation: transaction IDs wiped between connection opens

**File:** `test/suite/test_txn25.py`
**Storage mode:** General
**Components under test:** write generation, transaction ID reuse across restarts, `allocation_size=512`

## Test Cases

### `test_txn25.test_txn25`
- **What it tests:** Keeps a long-running transaction in session2 to pin transaction IDs; writes 3 waves of 999 rows to push transaction IDs high; forces all pages to disk via checkpoint; rolls back session2 (releasing the pin); reopens the connection (which resets transaction IDs to start from 1); reads all 999 rows and verifies they all show value3 (the latest committed value). Tests that write generation correctly clears stale transaction IDs on disk so the new low-numbered transaction can see the committed data.
- **Components:** `txn.c`, `btree.c`, `page.c`
- **Notes:** Parameterized over string-row/column × logging/no-logging (4 scenarios). Uses `file:` URI with `allocation_size=512`. Tests the write generation mechanism that ensures transaction IDs from a previous database run don't block visibility in the new run.
