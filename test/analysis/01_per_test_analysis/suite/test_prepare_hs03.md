# test_prepare_hs03 — Salvage and verify with prepared updates after crash

**File:** `test/suite/test_prepare_hs03.py`
**Storage mode:** General (skipped for tiered and disagg)
**Components under test:** prepared transactions, history store, salvage, verify, crash recovery

## Test Cases

### `test_prepare_hs03.test_prepare_hs`
- **What it tests:** Inserts data, prepares an update, optionally corrupts the database files, simulates a crash by copying to a RESTART directory, then runs salvage on the RESTART directory and verifies with `session.verify()`; verifies that the database is consistent after salvage regardless of whether corruption was injected
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `salvage/salvage.c`, `verify/verify.c`, `conn/conn_recover.c`
- **Notes:** Scenarios: corrupt/no-corrupt × column/integer-row; skipped for tiered and disagg hooks; the corrupt scenario deliberately corrupts specific bytes in a data file before salvage to test salvage's ability to handle partial corruption; the no-corrupt scenario verifies that salvage on a cleanly-crashed database (prepared updates not committed) correctly restores to a consistent state; after salvage, data at timestamps before the prepare is verified as readable
